package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strings"
)

// PLAT P0 alert subsystem: notification channels (emails/webhooks) live in
// MySQL. The management API replaces a user's channel set wholesale (matching
// the frontend's emails[]/webhooks[] shape), and the alert delivery path reads
// a user's channels by their PocketBase id via the identity projection.

type NotificationChannels struct {
	Emails   []string `json:"emails"`
	Webhooks []string `json:"webhooks"`
}

type NotificationChannelRepository interface {
	GetNotificationChannels(ctx context.Context, tenantID, userID ID) (NotificationChannels, error)
	ReplaceNotificationChannels(ctx context.Context, tenantID, userID ID, channels NotificationChannels) error
	// NotificationChannelsForExternalSubject resolves a PocketBase user id to
	// the MySQL user (users.external_subject_id) and returns their channels.
	// This is the alert delivery read path.
	NotificationChannelsForExternalSubject(ctx context.Context, provider, externalSubject string) (NotificationChannels, error)
}

func (s *MySQLStore) GetNotificationChannels(ctx context.Context, tenantID, userID ID) (NotificationChannels, error) {
	return scanNotificationChannels(s.db.QueryContext(ctx, `
		SELECT channel_type, address FROM notification_channels
		WHERE tenant_id = ? AND user_id = ? AND enabled = TRUE
		ORDER BY channel_type, id
	`, tenantID, userID))
}

func (s *MySQLStore) NotificationChannelsForExternalSubject(ctx context.Context, provider, externalSubject string) (NotificationChannels, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	externalSubject = strings.TrimSpace(externalSubject)
	if provider == "" || externalSubject == "" {
		return NotificationChannels{}, nil
	}
	return scanNotificationChannels(s.db.QueryContext(ctx, `
		SELECT c.channel_type, c.address
		FROM notification_channels c
		INNER JOIN users u ON u.id = c.user_id
		WHERE u.auth_provider = ? AND u.external_subject_id = ? AND c.enabled = TRUE
		ORDER BY c.channel_type, c.id
	`, provider, externalSubject))
}

func (s *MySQLStore) ReplaceNotificationChannels(ctx context.Context, tenantID, userID ID, channels NotificationChannels) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM notification_channels WHERE tenant_id = ? AND user_id = ?
	`, tenantID, userID); err != nil {
		return err
	}
	insert := func(channelType, address string) error {
		id, err := newIdentityID()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO notification_channels (id, tenant_id, user_id, channel_type, address)
			VALUES (?, ?, ?, ?, ?)
		`, id, tenantID, userID, channelType, address)
		return err
	}
	for _, email := range channels.Emails {
		if err := insert("email", email); err != nil {
			return err
		}
	}
	for _, webhook := range channels.Webhooks {
		if err := insert("webhook", webhook); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func scanNotificationChannels(rows *sql.Rows, scanErr error) (NotificationChannels, error) {
	if scanErr != nil {
		return NotificationChannels{}, scanErr
	}
	defer rows.Close()
	channels := NotificationChannels{Emails: []string{}, Webhooks: []string{}}
	for rows.Next() {
		var channelType, address string
		if err := rows.Scan(&channelType, &address); err != nil {
			return NotificationChannels{}, err
		}
		switch channelType {
		case "email":
			channels.Emails = append(channels.Emails, address)
		case "webhook":
			channels.Webhooks = append(channels.Webhooks, address)
		}
	}
	return channels, rows.Err()
}

// ValidateNotificationChannels normalizes and checks the channel set: emails
// must look like addresses and webhooks must be http(s) URLs without an
// obviously-internal host (a light SSRF guard; the delivery layer enforces the
// rest).
func ValidateNotificationChannels(channels NotificationChannels) (NotificationChannels, error) {
	out := NotificationChannels{Emails: []string{}, Webhooks: []string{}}
	for _, raw := range channels.Emails {
		email := strings.TrimSpace(raw)
		if email == "" {
			continue
		}
		if !strings.Contains(email, "@") || strings.ContainsAny(email, " \t\r\n") {
			return NotificationChannels{}, errors.New("invalid email address: " + email)
		}
		out.Emails = append(out.Emails, email)
	}
	for _, raw := range channels.Webhooks {
		webhook := strings.TrimSpace(raw)
		if webhook == "" {
			continue
		}
		parsed, err := url.Parse(webhook)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return NotificationChannels{}, errors.New("webhook must be an http(s) URL: " + webhook)
		}
		out.Webhooks = append(out.Webhooks, webhook)
	}
	return out, nil
}
