package main

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/mattermost/mattermost/server/public/plugin"
)

func (p *Plugin) initRouter() *mux.Router {
	router := mux.NewRouter()

	// /action — без middleware, авторизация через session_token внутри envelope
	router.HandleFunc("/api/v1/action", p.HandleAction).Methods(http.MethodPost)

	// /auth/google — тоже без middleware, но по другой причине: до входа у
	// клиента нет ни пользователя, ни сессии, и заголовку Mattermost-User-ID
	// взяться неоткуда (ADR-078 §2.4). Отдельный контракт, а не конверт
	// действий: тот требует session_token, которого здесь ещё не существует.
	router.HandleFunc("/api/v1/auth/google", p.HandleGoogleAuth).Methods(http.MethodPost)

	// остальные роуты — с проверкой Mattermost-User-ID
	authed := router.PathPrefix("/api/v1").Subrouter()
	authed.Use(p.MattermostAuthorizationRequired)
	authed.HandleFunc("/hello", p.HelloWorld).Methods(http.MethodGet)

	return router
}

func (p *Plugin) ServeHTTP(c *plugin.Context, w http.ResponseWriter, r *http.Request) {
	p.router.ServeHTTP(w, r)
}

func (p *Plugin) MattermostAuthorizationRequired(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get("Mattermost-User-ID")
		if userID == "" {
			http.Error(w, "Not authorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Plugin) HelloWorld(w http.ResponseWriter, r *http.Request) {
	if _, err := w.Write([]byte("Hello, world!")); err != nil {
		p.API.LogError("Failed to write response", "error", err)
	}
}

func (p *Plugin) HandleAction(w http.ResponseWriter, r *http.Request) {
	var envelope struct {
		Auth    AuthFrame       `json:"auth"`
		Header  HeaderFrame     `json:"header"`
		Payload json.RawMessage `json:"payload"`
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		json.NewEncoder(w).Encode(Response{OK: false, Error: "invalid envelope: " + err.Error()})
		return
	}

	// F6: единая точка проверки жизненного цикла сессии. Прежде здесь стоял
	// голый GetSession, который истечения не проверяет (ADR-039 §11).
	userID, appErr := authorizeSession(p.API, envelope.Auth.SessionToken)
	if appErr != nil {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(Response{OK: false, Error: "unauthorized"})
		return
	}

	// F7: тот же nil, что и в хуках — HTTP-вход разыменовывал listener без
	// проверки (api.go:67 в прежней редакции).
	if p.quicListener == nil || p.quicListener.dispatcher == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(Response{OK: false, Error: "photon bridge is not active"})
		return
	}

	resp := p.quicListener.dispatcher.Dispatch(userID, envelope.Header, envelope.Payload)
	_ = json.NewEncoder(w).Encode(resp)
}
