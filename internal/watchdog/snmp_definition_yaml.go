package watchdog

import "gopkg.in/yaml.v3"

func yamlUnmarshalImpl(data []byte, out any) error {
	return yaml.Unmarshal(data, out)
}
