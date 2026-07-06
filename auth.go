package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	oidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

func initOIDC() error {
	issuer := os.Getenv("OIDC_ISSUER")
	if issuer == "" {
		oidcEnabled = false
		return nil
	}

	clientID := os.Getenv("OIDC_CLIENT_ID")
	clientSecret := os.Getenv("OIDC_CLIENT_SECRET")
	redirectURL := os.Getenv("OIDC_REDIRECT_URL")
	if redirectURL == "" {
		redirectURL = "http://localhost:8080/auth/callback"
	}

	oidcAdminClaim = os.Getenv("OIDC_ADMIN_CLAIM")
	if oidcAdminClaim == "" {
		oidcAdminClaim = "groups"
	}
	oidcAdminValue = os.Getenv("OIDC_ADMIN_VALUE")
	if oidcAdminValue == "" {
		oidcAdminValue = "admin"
	}
	oidcSuperadminValue = os.Getenv("OIDC_SUPERADMIN_VALUE")
	if oidcSuperadminValue == "" {
		oidcSuperadminValue = "superadmin"
	}

	ttlStr := os.Getenv("OIDC_SESSION_TTL")
	if ttlStr == "" {
		ttlStr = "24h"
	}
	var err error
	sessionTTL, err = time.ParseDuration(ttlStr)
	if err != nil {
		sessionTTL = 24 * time.Hour
	}

	scopesStr := os.Getenv("OIDC_SCOPES")
	if scopesStr == "" {
		scopesStr = "openid,profile,email"
	}
	scopes := strings.Split(scopesStr, ",")
	for i := range scopes {
		scopes[i] = strings.TrimSpace(scopes[i])
	}

	ctx := context.Background()
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		log.Printf("WARNING: OIDC provider discovery failed: %v (OIDC disabled)", err)
		oidcEnabled = false
		return nil
	}

	oidcProvider = provider
	oidcVerifier = provider.Verifier(&oidc.Config{ClientID: clientID})
	oauth2Config = &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       scopes,
	}

	oidcEnabled = true
	log.Printf("OIDC authentication enabled (issuer: %s)", issuer)
	return nil
}

func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check API key first
		apiKey := r.Header.Get("X-API-Key")
		if apiKey != "" {
			var key APIKey
			err := db.Where("key = ?", apiKey).First(&key).Error
			if err != nil {
				http.Error(w, "Invalid API key", http.StatusForbidden)
				return
			}

			requestedPath := r.URL.Path
			allowed := false
			for _, endpoint := range strings.Split(key.AllowedEndpoints, ",") {
				if strings.TrimSpace(endpoint) == requestedPath {
					allowed = true
					break
				}
			}

			if !allowed {
				http.Error(w, "API key not authorized for this endpoint", http.StatusForbidden)
				return
			}

			ctx := context.WithValue(r.Context(), authContextKey, authInfo{
				Method: "apikey",
				Role:   "admin",
			})
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// Check OIDC session cookie
		if oidcEnabled {
			cookie, err := r.Cookie("session")
			if err == nil {
				var session Session
				err := db.Where("token = ? AND expires_at > ?", cookie.Value, time.Now()).First(&session).Error
				if err == nil {
					ctx := context.WithValue(r.Context(), authContextKey, authInfo{
						Method: "oidc",
						Role:   session.Role,
						Email:  session.Email,
					})
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}
		}

		http.Error(w, "Authentication required", http.StatusUnauthorized)
	}
}

// OIDC Auth Handlers

// @Summary Initiate OIDC login
// @Tags Auth
// @Success 302 {string} string "Redirect to OIDC provider"
// @Failure 404 {string} string "OIDC not configured"
// @Router /auth/login [get]
func authLoginHandler(w http.ResponseWriter, r *http.Request) {
	if !oidcEnabled {
		http.Error(w, "OIDC not configured", http.StatusNotFound)
		return
	}
	state := generateAPIKey()
	http.SetCookie(w, &http.Cookie{
		Name:     "oidc_state",
		Value:    state,
		Path:     "/",
		MaxAge:   300,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   strings.HasPrefix(oauth2Config.RedirectURL, "https://"),
	})
	http.Redirect(w, r, oauth2Config.AuthCodeURL(state), http.StatusFound)
}

// @Summary OIDC callback handler
// @Tags Auth
// @Success 302 {string} string "Redirect to /"
// @Failure 400 {string} string "Invalid state"
// @Router /auth/callback [get]
func authCallbackHandler(w http.ResponseWriter, r *http.Request) {
	if !oidcEnabled {
		http.Error(w, "OIDC not configured", http.StatusNotFound)
		return
	}

	// Validate state
	stateCookie, err := r.Cookie("oidc_state")
	if err != nil || stateCookie.Value != r.URL.Query().Get("state") {
		http.Error(w, "Invalid state", http.StatusBadRequest)
		return
	}

	// Exchange code for tokens
	oauth2Token, err := oauth2Config.Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		log.Printf("OIDC token exchange failed: %v", err)
		http.Error(w, "Token exchange failed", http.StatusInternalServerError)
		return
	}

	// Extract and verify ID token
	rawIDToken, ok := oauth2Token.Extra("id_token").(string)
	if !ok {
		http.Error(w, "No ID token in response", http.StatusInternalServerError)
		return
	}

	idToken, err := oidcVerifier.Verify(r.Context(), rawIDToken)
	if err != nil {
		log.Printf("OIDC ID token verification failed: %v", err)
		http.Error(w, "Invalid ID token", http.StatusInternalServerError)
		return
	}

	// Extract claims
	var claims map[string]interface{}
	if err := idToken.Claims(&claims); err != nil {
		http.Error(w, "Failed to parse claims", http.StatusInternalServerError)
		return
	}

	email, _ := claims["email"].(string)
	name, _ := claims["name"].(string)
	sub := idToken.Subject

	// Determine role
	role := "user"
	if claimValue, ok := claims[oidcAdminClaim]; ok {
		switch v := claimValue.(type) {
		case string:
			if v == oidcSuperadminValue {
				role = "superadmin"
			} else if v == oidcAdminValue {
				role = "admin"
			}
		case []interface{}:
			for _, item := range v {
				if str, ok := item.(string); ok && str == oidcSuperadminValue {
					role = "superadmin"
					break
				}
				if str, ok := item.(string); ok && str == oidcAdminValue {
					role = "admin"
				}
			}
		}
	}

	// Create session
	sessionToken := generateAPIKey()
	session := Session{
		Token:     sessionToken,
		Email:     email,
		Name:      name,
		Subject:   sub,
		Role:      role,
		ExpiresAt: time.Now().Add(sessionTTL),
	}
	db.Create(&session)

	// Set session cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "session",
		Value:    sessionToken,
		Path:     "/",
		MaxAge:   int(sessionTTL.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   strings.HasPrefix(oauth2Config.RedirectURL, "https://"),
	})

	// Clear state cookie
	http.SetCookie(w, &http.Cookie{
		Name:   "oidc_state",
		Path:   "/",
		MaxAge: -1,
	})

	http.Redirect(w, r, "/", http.StatusFound)
}

// @Summary Logout and clear session
// @Tags Auth
// @Success 200 {string} string "OK"
// @Router /auth/logout [post]
func authLogoutHandler(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("session")
	if err == nil {
		db.Where("token = ?", cookie.Value).Delete(&Session{})
	}

	// Clear session cookie
	http.SetCookie(w, &http.Cookie{
		Name:   "session",
		Path:   "/",
		MaxAge: -1,
	})

	w.WriteHeader(http.StatusOK)
}

// @Summary Get current session info
// @Tags Auth
// @Produce json
// @Success 200 {object} map[string]interface{} "Session info"
// @Failure 401 {string} string "Not authenticated"
// @Router /auth/me [get]
func authMeHandler(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("session")
	if err != nil {
		http.Error(w, "Not authenticated", http.StatusUnauthorized)
		return
	}

	var session Session
	err = db.Where("token = ? AND expires_at > ?", cookie.Value, time.Now()).First(&session).Error
	if err != nil {
		http.Error(w, "Session expired", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"authenticated": true,
		"email":         session.Email,
		"name":          session.Name,
		"role":          session.Role,
		"is_superadmin": session.Role == "superadmin",
	})
}

func startSessionCleanup() {
	go func() {
		for {
			time.Sleep(1 * time.Hour)
			result := db.Where("expires_at < ?", time.Now()).Delete(&Session{})
			if result.RowsAffected > 0 {
				log.Printf("Cleaned up %d expired sessions", result.RowsAffected)
			}
		}
	}()
}
