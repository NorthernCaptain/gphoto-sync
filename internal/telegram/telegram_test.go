package telegram

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gphoto-sync/internal/db"
	"gphoto-sync/internal/store"
)

type fakeAPI struct {
	mu      sync.Mutex
	updates []Update
	sent    []map[string]any
}

func (f *fakeAPI) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	var params map[string]any
	json.NewDecoder(r.Body).Decode(&params)
	write := func(v any) {
		b, _ := json.Marshal(v)
		w.Write([]byte(`{"ok":true,"result":` + string(b) + `}`))
	}
	switch method {
	case "getMe":
		write(User{ID: 1, Username: "testbot"})
	case "getUpdates":
		off := int64(params["offset"].(float64))
		var out []Update
		for _, u := range f.updates {
			if u.UpdateID >= off {
				out = append(out, u)
			}
		}
		write(out)
	case "sendMessage":
		f.sent = append(f.sent, params)
		write(map[string]any{"message_id": 1})
	default:
		w.Write([]byte(`{"ok":false,"description":"unknown"}`))
	}
}

func TestPairAndDrain(t *testing.T) {
	api := &fakeAPI{}
	srv := httptest.NewServer(http.HandlerFunc(api.handler))
	defer srv.Close()
	ctx := context.Background()
	d, err := db.Open(ctx, filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	st := store.New(d)
	bot := &Bot{C: NewClient("tok", srv.URL), St: st, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	// capture the code from the pairing output by intercepting Pair's writer
	pr, pw := io.Pipe()
	codeCh := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(pr)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if strings.HasPrefix(line, "/start ") {
				codeCh <- strings.TrimPrefix(line, "/start ")
				break
			}
		}
		io.Copy(io.Discard, pr)
	}()
	go func() {
		code := <-codeCh
		api.mu.Lock()
		api.updates = append(api.updates,
			Update{UpdateID: 10, Message: &Message{Chat: Chat{ID: 5, Type: "private"}, From: &User{ID: 7, Username: "alice"}, Text: "/start WRONG"}},
			Update{UpdateID: 11, Message: &Message{Chat: Chat{ID: 6, Type: "group"}, Text: "/start " + code}},
			Update{UpdateID: 12, Message: &Message{Chat: Chat{ID: 5, Type: "private"}, From: &User{ID: 7, Username: "alice", FirstName: "Alice"}, Text: "/start " + code}},
		)
		api.mu.Unlock()
	}()
	n, err := bot.Pair(ctx, 1, 10*time.Second, pw)
	pw.Close()
	if err != nil || n != 1 {
		t.Fatalf("pair: %d %v", n, err)
	}
	chats, _ := st.PairedChats(ctx)
	if len(chats) != 1 || chats[0].ChatID != 5 {
		t.Fatalf("chats %+v", chats)
	}
	// the wrong code and the group attempt were refused
	api.mu.Lock()
	refused := 0
	for _, s := range api.sent {
		if txt, _ := s["text"].(string); strings.Contains(txt, "Invalid") || strings.Contains(txt, "private") {
			refused++
		}
	}
	api.updates = append(api.updates,
		Update{UpdateID: 13, Message: &Message{Chat: Chat{ID: 9, Type: "private"}, Text: "/start " + "ANYTHING"}},
		Update{UpdateID: 14, Message: &Message{Chat: Chat{ID: 5, Type: "private"}, Text: "/stop"}},
	)
	api.mu.Unlock()
	if refused != 2 {
		t.Errorf("refusals %d want 2", refused)
	}
	if err := bot.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	chats, _ = st.PairedChats(ctx)
	if len(chats) != 0 {
		t.Errorf("chat not revoked: %+v", chats)
	}
	if v, _ := st.TelegramState(ctx, offsetKey); v != "15" {
		t.Errorf("offset %q want 15", v)
	}
	sent, err := bot.Broadcast(ctx, "<b>x</b>")
	if err != nil || sent != 0 {
		t.Errorf("broadcast to revoked: %d %v", sent, err)
	}
}

func TestSplit(t *testing.T) {
	text := strings.Repeat("line of text\n", 400)
	parts := Split(text, 500)
	for _, p := range parts {
		if len(p) > 500 || !strings.HasSuffix(p, "\n") {
			t.Errorf("bad chunk %d", len(p))
		}
	}
	if strings.Join(parts, "") != text {
		t.Error("chunks do not reassemble")
	}
	if got := Split(strings.Repeat("x", 1200), 500); len(got) != 3 {
		t.Errorf("hard cut: %d chunks", len(got))
	}
}
