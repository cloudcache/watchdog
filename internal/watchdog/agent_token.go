package watchdog

import "golang.org/x/crypto/bcrypt"

func NewAgentTokenHash(token string) string {
	hash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return ""
	}
	return string(hash)
}

func AgentTokenMatches(token string, hash string) bool {
	if token == "" || hash == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(token)) == nil
}
