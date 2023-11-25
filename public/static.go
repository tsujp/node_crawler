package public

import _ "embed"

//go:embed blue-marble.png
var blueMarble []byte

//go:embed eth-diamond-purple.png
var ethDiamondPurple []byte

//go:embed favicon.ico
var Favicon []byte

var StaticFiles = map[string][]byte{
	"blue-marble.png":        blueMarble,
	"eth-diamond-purple.png": ethDiamondPurple,
	"favicon.ico":            Favicon,
}
