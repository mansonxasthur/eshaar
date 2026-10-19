package domain

import "fmt"

// Channel is a delivery channel. One notification row exists per channel.
type Channel string

// The supported channels.
const (
	ChannelEmail    Channel = "email"
	ChannelSMS      Channel = "sms"
	ChannelWhatsApp Channel = "whatsapp"
)

// Channels returns every supported channel.
func Channels() []Channel {
	return []Channel{ChannelEmail, ChannelSMS, ChannelWhatsApp}
}

// ParseChannel returns the channel named s, or an error if it is unknown.
func ParseChannel(s string) (Channel, error) {
	for _, c := range Channels() {
		if string(c) == s {
			return c, nil
		}
	}
	return "", fmt.Errorf("unknown channel %q", s)
}
