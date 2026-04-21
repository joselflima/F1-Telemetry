package main

import (
	"joselucas/f1-telemetry/src/utils"
)

type TelemetryPayload struct {
	CarIndex                uint8      `json:"car_index"`
	Speed                   uint16     `json:"speed"`
	Throttle                float32    `json:"throttle"`
	Steer                   float32    `json:"steer"`
	Brake                   float32    `json:"brake"`
	Clutch                  uint8      `json:"clutch"`
	Gear                    int8       `json:"gear"`
	EngineRPM               uint16     `json:"engine_rpm"`
	DRS                     uint8      `json:"drs"`
	RevLightsPercent        uint8      `json:"rev_lights_percent"`
	RevLightsBitValue       uint16     `json:"rev_lights_bit_value"`
	EngineTemperature       uint16     `json:"engine_temperature"`
	BrakesTemperature       [4]uint16  `json:"brakes_temperature"`
	TyresSurfaceTemperature [4]uint8   `json:"tyres_surface_temperature"`
	TyresInnerTemperature   [4]uint8   `json:"tyres_inner_temperature"`
	TyresPressure           [4]float32 `json:"tyres_pressure"`
	SurfaceType             [4]uint8   `json:"surface_type"`
}

func BuildTelemetryPayload(packet *PacketCarTelemetryData, carIndex uint8) TelemetryPayload {
	data := packet.CarTelemetryData[carIndex]

	payload := TelemetryPayload{
		CarIndex:                carIndex,
		Speed:                   data.Speed,
		Throttle:                data.Throttle,
		Steer:                   data.Steer,
		Brake:                   data.Brake,
		Clutch:                  data.Clutch,
		Gear:                    data.Gear,
		EngineRPM:               data.EngineRPM,
		DRS:                     data.DRS,
		RevLightsPercent:        data.RevLightsPercent,
		RevLightsBitValue:       data.RevLightsBitValue,
		EngineTemperature:       data.EngineTemperature,
		BrakesTemperature:       data.BrakesTemperature,
		TyresSurfaceTemperature: data.TyresSurfaceTemperature,
		TyresInnerTemperature:   data.TyresInnerTemperature,
		TyresPressure:           data.TyresPressure,
		SurfaceType:             data.SurfaceType,
	}

	utils.Logger.Printf("Payload pronto para envio ao Kafka: Velocidade=%d, Throttle=%f, Gear=%d, Brake=%f", data.Speed, data.Throttle, data.Gear, data.Brake)
	return payload
}
