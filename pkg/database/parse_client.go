package database

import (
	"strings"
	"unicode"

	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/mod/semver"
)

type Client struct {
	Name     string
	Version  string
	OS       string
	Arch     string
	Language string
}

func parseOSArch(osStr string) (string, string) {
	if osStr == "" {
		return Unknown, Unknown
	}

	parts := strings.FieldsFunc(osStr, func(c rune) bool {
		return c == '-'
	})

	var os, arch string

	for _, part := range parts {
		switch part {
		case "musl", "unknown", "gnu":
			// NOOP
		case "linux":
			os = "Linux"
		case "freebsd":
			os = "FreeBSD"
		case "android":
			os = "Android"
		case "windows", "win32":
			os = "Windows"
		case "darwin", "osx", "macos", "apple":
			os = "MacOS"
		case "amd64", "x64", "x86_64":
			arch = "amd64"
		case "arm64", "aarch_64", "aarch64", "arm":
			arch = "arm64"
		case "386":
			arch = "i386"
		case "s390x":
			arch = "IBM System/390"
		default:
			// NOOP
		}
	}

	if os == "" {
		os = Unknown
	}

	if arch == "" {
		arch = Unknown
	}

	return os, arch
}

func isVersion(version string) bool {
	return semver.IsValid(version)
}

func parseClientName(clientName *string) *Client {
	if clientName == nil {
		return nil
	}

	name := strings.ToLower(*clientName)

	if name == "" {
		return nil
	}

	if name == "server" {
		return nil
	}

	if strings.HasPrefix(name, "nimbus-eth1") {
		newClientName := make([]rune, 0, len(name))
		for _, c := range name {
			switch c {
			case '[', ']', ':', ',':
				// NOOP
			default:
				newClientName = append(newClientName, c)
			}
		}

		parts := strings.Split(string(newClientName), " ")

		if len(parts) != 7 {
			log.Error("nimbus-eth1 not valid", "client_name", name)
		}

		os, arch := parseOSArch(parts[2])
		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       os,
			Arch:     arch,
			Language: "nim",
		}
	}

	parts := strings.Split(strings.ToLower(name), "/")

	if parts[0] == "" {
		return nil
	}

	switch len(parts) {
	case 1:
		return &Client{
			Name:     parts[0],
			Version:  Unknown,
			OS:       Unknown,
			Arch:     Unknown,
			Language: Unknown,
		}
	case 2:
		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       Unknown,
			Arch:     Unknown,
			Language: Unknown,
		}
	case 3:
		var os, arch, lang string

		if parts[0] == "reth" {
			lang = "rust"
			os, arch = parseOSArch(parts[2])
		} else if parts[0] == "geth" {
			lang = "go"

			if isVersion(parts[1]) {
				os, arch = parseOSArch(parts[2])
			} else {
				os, arch = parseOSArch(parts[1])
				lang = parts[2]
			}
		} else {
			log.Error("not reth or geth", "client_name", name)
		}

		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       os,
			Arch:     arch,
			Language: lang,
		}
	case 4:
		os, arch := parseOSArch(parts[2])
		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       os,
			Arch:     arch,
			Language: parts[3],
		}
	case 5:
		var version, os, arch, lang string

		// handle geth/v1.2.11-e3acd735-20231031/linux-amd64/go1.20.5/{d+}
		if strings.TrimFunc(parts[4], unicode.IsDigit) == "" {
			version = parts[1]
			os, arch = parseOSArch(parts[2])
			lang = parts[3]
		} else {
			version = parts[2]
			os, arch = parseOSArch(parts[3])
			lang = parts[4]
		}

		return &Client{
			Name:     parts[0],
			Version:  version,
			OS:       os,
			Arch:     arch,
			Language: lang,
		}
	case 6:
		if parts[0] == "q-client" {
			os, arch := parseOSArch(parts[4])
			return &Client{
				Name:     parts[0],
				Version:  parts[1],
				OS:       os,
				Arch:     arch,
				Language: parts[5],
			}
		}
	case 7:
		os, arch := parseOSArch(parts[5])
		return &Client{
			Name:     parts[0],
			Version:  parts[4],
			OS:       os,
			Arch:     arch,
			Language: parts[6],
		}
	}

	log.Error("could not parse client", "client_name", name)

	return nil
}
