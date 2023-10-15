package crawlerv2

var hexAlpha = "0123456789abcdef"

type nodeIDRange struct {
	start string
	end   string
}

func range16() []nodeIDRange {
	out := make([]nodeIDRange, 16)

	for i := 0; i < 16; i++ {
		prefix := string([]byte{hexAlpha[i]})

		out[i] = nodeIDRange{
			start: nodeIDString(prefix, '0'),
			end:   nodeIDString(prefix, 'f'),
		}
	}

	return out
}

func range8() []nodeIDRange {
	out := make([]nodeIDRange, 8)

	for i := 0; i < 8; i++ {
		start := string([]byte{hexAlpha[i*2]})
		end := string([]byte{hexAlpha[i*2+1]})

		out[i] = nodeIDRange{
			start: nodeIDString(start, '0'),
			end:   nodeIDString(end, 'f'),
		}
	}

	return out
}

func range4() []nodeIDRange {
	out := make([]nodeIDRange, 4)

	for i := 0; i < 4; i++ {
		start := string([]byte{hexAlpha[i*4]})
		end := string([]byte{hexAlpha[i*4+3]})

		out[i] = nodeIDRange{
			start: nodeIDString(start, '0'),
			end:   nodeIDString(end, 'f'),
		}
	}

	return out
}

func range2() []nodeIDRange {
	return []nodeIDRange{
		{
			start: nodeIDString("0", '0'),
			end:   nodeIDString("7", 'f'),
		},
		{
			start: nodeIDString("8", '0'),
			end:   nodeIDString("f", 'f'),
		},
	}
}

func range1() []nodeIDRange {
	return []nodeIDRange{
		{
			start: nodeIDString("0", '0'),
			end:   nodeIDString("f", 'f'),
		},
	}
}

func rangeN(n uint64) []nodeIDRange {
	switch n {
	case 1:
		return range1()
	case 2:
		return range2()
	case 4:
		return range4()
	case 8:
		return range8()
	case 16:
		return range16()
	default:
		panic("invalid num crawler range")
	}
}
