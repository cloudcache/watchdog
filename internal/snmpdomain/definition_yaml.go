package snmpdomain

import "gopkg.in/yaml.v3"

type librenmsStringList []string

func (list *librenmsStringList) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		*list = nil
		return nil
	}
	if node.Kind == yaml.ScalarNode {
		value := node.Value
		if value == "" {
			*list = nil
		} else {
			*list = librenmsStringList{value}
		}
		return nil
	}
	if node.Kind != yaml.SequenceNode {
		return &yaml.TypeError{Errors: []string{"expected a string or list of strings"}}
	}
	values := make(librenmsStringList, 0, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.ScalarNode {
			return &yaml.TypeError{Errors: []string{"expected a string or list of strings"}}
		}
		value := item.Value
		if value != "" {
			values = append(values, value)
		}
	}
	*list = values
	return nil
}

func yamlUnmarshalImpl(data []byte, out any) error {
	return yaml.Unmarshal(data, out)
}
