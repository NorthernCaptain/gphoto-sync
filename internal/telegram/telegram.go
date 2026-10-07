// Package telegram is a minimal Bot API client plus the pairing and report
// logic described in PLAN.md 3.9. Private chats only; pairing uses single-use
// codes shown in the admin's terminal.
package telegram

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gphoto-sync/internal/store"
)

// Client talks to api.telegram.org for one bot.
type Client struct {
	token string
	base  string
	http  *http.Client
}

// NewClient creates a client; base may be overridden for tests.
func NewClient(token, base string) *Client {
	if base == "" {
		base = "https://api.telegram.org"
	}
	return &Client{token: token, base: base, http: &http.Client{Timeout: 70 * time.Second}}
}

// ReadToken reads and trims a token file.
func ReadToken(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	t := strings.TrimSpace(string(b))
	if t == "" {
		return "", errors.New("telegram token file is empty")
	}
	return t, nil
}

// User is a Telegram user or bot.
type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

// Chat is a Telegram chat.
type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

// Message is an incoming message.
type Message struct {
	MessageID int64  `json:"message_id"`
	From      *User  `json:"from"`
	Chat      Chat   `json:"chat"`
	Text      string `json:"text"`
}

// Update is one getUpdates item.
type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

type apiResp struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
}

func (c *Client) call(ctx context.Context, method string, params any, out any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/bot"+c.token+"/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		// never return the request URL: it carries the bot token
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("telegram %s: %w", method, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	var r apiResp
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("telegram %s: bad response: %w", method, err)
	}
	if !r.OK {
		return fmt.Errorf("telegram %s: %s", method, r.Description)
	}
	if out != nil {
		return json.Unmarshal(r.Result, out)
	}
	return nil
}

// GetMe returns the bot's identity.
func (c *Client) GetMe(ctx context.Context) (*User, error) {
	var u User
	if err := c.call(ctx, "getMe", map[string]any{}, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

// GetUpdates long-polls for updates after offset.
func (c *Client) GetUpdates(ctx context.Context, offset int64, timeoutSec int) ([]Update, error) {
	var ups []Update
	err := c.call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": timeoutSec, "allowed_updates": []string{"message"}}, &ups)
	return ups, err
}

// MaxMessage is the safe length of one HTML message (Telegram's limit is 4096).
const MaxMessage = 3500

// SendMessage sends HTML text, split on line boundaries when long.
func (c *Client) SendMessage(ctx context.Context, chatID int64, htmlText string) error {
	for _, chunk := range Split(htmlText, MaxMessage) {
		if err := c.call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": chunk, "parse_mode": "HTML", "disable_web_page_preview": true}, nil); err != nil {
			return err
		}
	}
	return nil
}

// Split cuts text into chunks of at most limit bytes on line boundaries.
func Split(text string, limit int) []string {
	if len(text) <= limit {
		return []string{text}
	}
	var out []string
	var cur strings.Builder
	for _, line := range strings.SplitAfter(text, "\n") {
		for len(line) > limit { // a single overlong line is cut hard
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
			out = append(out, line[:limit])
			line = line[limit:]
		}
		if cur.Len()+len(line) > limit {
			out = append(out, cur.String())
			cur.Reset()
		}
		cur.WriteString(line)
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// Esc escapes text for HTML parse mode.
func Esc(s string) string { return html.EscapeString(s) }

// ---- bot logic ------------------------------------------------------------

// Bot combines the client with stored state.
type Bot struct {
	C   *Client
	St  *store.Store
	Log *slog.Logger
}

const offsetKey = "last_update_id"

func (b *Bot) offset(ctx context.Context) int64 {
	v, _ := b.St.TelegramState(ctx, offsetKey)
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

func (b *Bot) setOffset(ctx context.Context, updateID int64) {
	_ = b.St.SetTelegramState(ctx, offsetKey, strconv.FormatInt(updateID+1, 10))
}

const codeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" // 32 symbols, no 0/O/1/I

// NewCode returns a random 8-character pairing code.
func NewCode() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}
	out := make([]byte, 8)
	for i, v := range buf {
		out[i] = codeAlphabet[int(v)%len(codeAlphabet)]
	}
	return string(out)
}

// Pair runs an interactive pairing session: prints `people` single-use codes
// and accepts "/start <code>" from private chats until all are used, the
// timeout passes, or ctx is cancelled.
func (b *Bot) Pair(ctx context.Context, people int, timeout time.Duration, out io.Writer) (paired int, err error) {
	codes := map[string]bool{}
	for i := 0; i < people; i++ {
		codes[NewCode()] = false
	}
	me, err := b.C.GetMe(ctx)
	if err != nil {
		return 0, err
	}
	fmt.Fprintf(out, "Bot @%s is waiting for %d person(s). Each opens a private chat with the bot and sends:\n", me.Username, people)
	for code := range codes {
		fmt.Fprintf(out, "   /start %s\n", code)
	}
	fmt.Fprintf(out, "Codes are single-use and expire when this command exits (%s).\n", timeout)
	deadline := time.Now().Add(timeout)
	for paired < people && time.Now().Before(deadline) && ctx.Err() == nil {
		ups, err := b.C.GetUpdates(ctx, b.offset(ctx), 25)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			b.Log.Warn("getUpdates failed", "err", err)
			time.Sleep(5 * time.Second)
			continue
		}
		for _, u := range ups {
			b.setOffset(ctx, u.UpdateID)
			m := u.Message
			if m == nil || m.Text == "" {
				continue
			}
			switch {
			case strings.HasPrefix(m.Text, "/start"):
				if m.Chat.Type != "private" {
					b.reply(ctx, m.Chat.ID, "Pairing works in a private chat with the bot only.")
					continue
				}
				code := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(m.Text, "/start")))
				used, known := codes[code]
				if !known || used {
					b.Log.Warn("invalid pairing code", "chat", m.Chat.ID, "user", userName(m.From))
					b.reply(ctx, m.Chat.ID, "Invalid code.")
					continue
				}
				if err := b.St.PairChat(ctx, m.Chat.ID, userID(m.From), userName(m.From), displayName(m.From)); err != nil {
					return paired, err
				}
				codes[code] = true
				paired++
				fmt.Fprintf(out, "paired chat %d (%s)\n", m.Chat.ID, displayName(m.From))
				b.reply(ctx, m.Chat.ID, "Paired. You will receive gphoto-sync reports here. Send /stop to unsubscribe.")
			case strings.HasPrefix(m.Text, "/stop"):
				_ = b.St.RevokeChat(ctx, m.Chat.ID)
				b.reply(ctx, m.Chat.ID, "Unsubscribed.")
			default:
				b.reply(ctx, m.Chat.ID, "Send /start <code> with the code shown by the admin, or /stop to unsubscribe.")
			}
		}
	}
	return paired, nil
}

// Drain processes updates that arrived since the last run: /stop is honoured,
// /start is refused (pairing is closed), and the offset advances.
func (b *Bot) Drain(ctx context.Context) error {
	for {
		ups, err := b.C.GetUpdates(ctx, b.offset(ctx), 0)
		if err != nil {
			return err
		}
		if len(ups) == 0 {
			return nil
		}
		for _, u := range ups {
			if m := u.Message; m != nil {
				switch {
				case strings.HasPrefix(m.Text, "/stop"):
					// persist the revocation before the offset moves past this update
					if err := b.St.RevokeChat(ctx, m.Chat.ID); err != nil {
						return fmt.Errorf("revoke chat %d: %w", m.Chat.ID, err)
					}
					b.reply(ctx, m.Chat.ID, "Unsubscribed.")
				case strings.HasPrefix(m.Text, "/start"):
					b.reply(ctx, m.Chat.ID, "Pairing is closed. Ask the admin to run: gphoto-sync telegram pair")
				}
			}
			b.setOffset(ctx, u.UpdateID)
		}
	}
}

// Broadcast sends an HTML message to every paired chat.
func (b *Bot) Broadcast(ctx context.Context, htmlText string) (sent int, err error) {
	chats, err := b.St.PairedChats(ctx)
	if err != nil {
		return 0, err
	}
	var errs []error
	for _, ch := range chats {
		if err := b.C.SendMessage(ctx, ch.ChatID, htmlText); err != nil {
			errs = append(errs, fmt.Errorf("chat %d: %w", ch.ChatID, err))
			continue
		}
		sent++
	}
	return sent, errors.Join(errs...)
}

func (b *Bot) reply(ctx context.Context, chatID int64, text string) {
	if err := b.C.SendMessage(ctx, chatID, Esc(text)); err != nil {
		b.Log.Warn("reply failed", "chat", chatID, "err", err)
	}
}

func userID(u *User) int64 {
	if u == nil {
		return 0
	}
	return u.ID
}

func userName(u *User) string {
	if u == nil {
		return ""
	}
	return u.Username
}

func displayName(u *User) string {
	if u == nil {
		return ""
	}
	return strings.TrimSpace(u.FirstName + " " + u.LastName)
}
