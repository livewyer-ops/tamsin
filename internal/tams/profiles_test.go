package tams

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProfilesFollowsPaging(t *testing.T) {
	t.Parallel()
	const (
		firstID  = "11111111-1111-4111-8111-111111111111"
		secondID = "22222222-2222-4222-8222-222222222222"
	)
	var baseURL string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/service/profiles" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Query().Get("cursor") == "next" {
			_, _ = io.WriteString(writer, `[{"id":"`+secondID+`","flow_metadata":{"format":"urn:x-nmos:format:video"}}]`)
			return
		}
		if request.URL.Query().Get("format") != "urn:x-nmos:format:video" ||
			request.URL.Query().Get("codec") != "video/h264" || request.URL.Query().Get("label") != "house" {
			http.Error(writer, "missing filters", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Link", "<"+baseURL+"/api/service/profiles?cursor=next>; rel=\"next\"")
		_, _ = io.WriteString(writer, `[{"id":"`+firstID+`","flow_metadata":{"format":"urn:x-nmos:format:video"}}]`)
	}))
	baseURL = server.URL
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL + "/api", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := client.Profiles(context.Background(), ProfileListOptions{
		Format: "urn:x-nmos:format:video", Codec: "video/h264", Label: "house",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 || profiles[0]["id"] != firstID || profiles[1]["id"] != secondID {
		t.Fatalf("Profiles() = %#v", profiles)
	}
}

func TestClientFlowProfileOperations(t *testing.T) {
	t.Parallel()
	const profileID = "33333333-3333-4333-8333-333333333333"
	var posted Profile
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/service/profiles/"+profileID {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case http.MethodGet:
			_, _ = io.WriteString(writer, `{"id":"`+profileID+`","label":"house","flow_metadata":{"format":"urn:x-nmos:format:audio"}}`)
		case http.MethodPost:
			if err := json.NewDecoder(request.Body).Decode(&posted); err != nil {
				t.Errorf("decode profile: %v", err)
			}
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(posted)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := client.Profile(context.Background(), profileID)
	if err != nil || profile["label"] != "house" {
		t.Fatalf("Profile() = %#v, %v", profile, err)
	}
	created, err := client.CreateProfile(context.Background(), profileID, Profile{
		"id": profileID, "label": "archive", "flow_metadata": map[string]any{"format": "urn:x-nmos:format:audio"},
	})
	if err != nil || created["label"] != "archive" || posted["id"] != profileID {
		t.Fatalf("CreateProfile() = %#v, %v; posted %#v", created, err, posted)
	}
}
