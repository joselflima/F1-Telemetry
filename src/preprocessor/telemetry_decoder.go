package main

import (
	"bytes"
	"encoding/binary"
)

type PacketHeader struct {
	PacketFormat            uint16
	GameYear                uint8
	GameMajorVersion        uint8
	GameMinorVersion        uint8
	PacketVersion           uint8
	PacketID                uint8
	SessionUID              uint64
	SessionTime             float32
	FrameIdentifier         uint32
	OverallFrameIdentifier  uint32
	PlayerCarIndex          uint8
	SecondaryPlayerCarIndex uint8
}

type CarTelemetryData struct {
	Speed                   uint16
	Throttle                float32
	Steer                   float32
	Brake                   float32
	Clutch                  uint8
	Gear                    int8
	EngineRPM               uint16
	DRS                     uint8
	RevLightsPercent        uint8
	RevLightsBitValue       uint16
	BrakesTemperature       [4]uint16
	TyresSurfaceTemperature [4]uint8
	TyresInnerTemperature   [4]uint8
	EngineTemperature       uint16
	TyresPressure           [4]float32
	SurfaceType             [4]uint8
}

type PacketCarTelemetryData struct {
	Header                       PacketHeader
	CarTelemetryData             [22]CarTelemetryData
	MFDPanelIndex                uint8
	MFDPanelIndexSecondaryPlayer uint8
	SuggestedGear                int8
}

func DecodeTelemetryPacket(reader *bytes.Reader) (*PacketCarTelemetryData, error) {
	reader.Seek(0, 0)
	var telemetryPacket PacketCarTelemetryData
	if err := binary.Read(reader, byteOrder, &telemetryPacket); err != nil {
		return nil, err
	}
	return &telemetryPacket, nil
}
