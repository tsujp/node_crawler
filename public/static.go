package public

import "embed"

//go:embed favicon.ico
var Favicon []byte

//go:embed blue-marble.png eth-diamond-purple.png style.css
var StaticFiles embed.FS
