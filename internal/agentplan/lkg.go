package agentplan

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

var (
	ErrNoLKG       = errors.New("no agent plan LKG is available")
	ErrLKGRollback = errors.New("agent plan LKG version rollback")
	ErrLKGConflict = errors.New("agent plan LKG version conflict")
)

type DiskLKG struct{ Path string }

func (l DiskLKG) Load(publicKey ed25519.PublicKey, agentID, kind, apiVersion string, capabilities []string) ([]byte, Envelope, Spec, error) {
	if strings.TrimSpace(l.Path) == "" {
		return nil, Envelope{}, Spec{}, ErrNoLKG
	}
	data, err := os.ReadFile(l.Path)
	if os.IsNotExist(err) {
		return nil, Envelope{}, Spec{}, ErrNoLKG
	}
	if err != nil {
		return nil, Envelope{}, Spec{}, err
	}
	envelope, spec, err := Verify(data, publicKey /* historical LKG */, zeroTime, false)
	if err != nil {
		return nil, Envelope{}, Spec{}, fmt.Errorf("verify agent plan LKG: %w", err)
	}
	if envelope.Metadata.AgentID != agentID {
		return nil, Envelope{}, Spec{}, errors.New("agent plan LKG belongs to a different agent")
	}
	if err := ValidateCompatibility(spec, agentID, kind, apiVersion, capabilities); err != nil {
		return nil, Envelope{}, Spec{}, err
	}
	return data, envelope, spec, nil
}

func (l DiskLKG) Install(data []byte, publicKey ed25519.PublicKey, agentID, kind, apiVersion string, capabilities []string) (Envelope, Spec, error) {
	return l.InstallAt(data, publicKey, agentID, kind, apiVersion, capabilities, nowUTC())
}

func (l DiskLKG) InstallAt(data []byte, publicKey ed25519.PublicKey, agentID, kind, apiVersion string, capabilities []string, now time.Time) (Envelope, Spec, error) {
	envelope, spec, err := Verify(data, publicKey, now, true)
	if err != nil {
		return Envelope{}, Spec{}, err
	}
	if envelope.Metadata.AgentID != agentID {
		return Envelope{}, Spec{}, errors.New("agent plan belongs to a different agent")
	}
	if err := ValidateCompatibility(spec, agentID, kind, apiVersion, capabilities); err != nil {
		return Envelope{}, Spec{}, err
	}
	if currentData, current, _, loadErr := l.Load(publicKey, agentID, kind, apiVersion, capabilities); loadErr == nil {
		switch {
		case envelope.Metadata.PlanVersion < current.Metadata.PlanVersion:
			return Envelope{}, Spec{}, ErrLKGRollback
		case envelope.Metadata.PlanVersion == current.Metadata.PlanVersion && string(data) != string(currentData):
			return Envelope{}, Spec{}, ErrLKGConflict
		case envelope.Metadata.PlanVersion == current.Metadata.PlanVersion:
			return envelope, spec, nil
		}
	} else if !errors.Is(loadErr, ErrNoLKG) {
		return Envelope{}, Spec{}, loadErr
	}
	if strings.TrimSpace(l.Path) == "" {
		return Envelope{}, Spec{}, errors.New("agent plan LKG path is required")
	}
	if err := atomicWriteFile(l.Path, data, 0o600); err != nil {
		return Envelope{}, Spec{}, fmt.Errorf("persist agent plan LKG: %w", err)
	}
	return envelope, spec, nil
}
