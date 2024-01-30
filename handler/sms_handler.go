package handler

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"github.com/twilio/twilio-go"
	api "github.com/twilio/twilio-go/rest/api/v2010"
)

type SMSHandler struct {
	twilioClient              *twilio.RestClient
	virtualAppTelephoneNumber string
}

type SMSNotification struct {
	TelephoneNumber string
	Message         string
}

func NewSMSHandler(twilioClient *twilio.RestClient,
	virtualAppTelephoneNumber string) *SMSHandler {
	return &SMSHandler{
		twilioClient:              twilioClient,
		virtualAppTelephoneNumber: virtualAppTelephoneNumber,
	}
}

func (s *SMSHandler) ParseData(rawData []byte) (*SMSNotification, *Error) {
	var smsData SMSNotification
	err := json.Unmarshal(rawData, &smsData)
	if err != nil {
		return nil, &Error{
			ErrorType: NON_RECOVERABLE,
		}
	}

	return &smsData, nil
}

func (s *SMSHandler) SendNotification(notification *SMSNotification) *Error {
	params := s.getSmsParams(notification)
	resp, err := s.twilioClient.Api.CreateMessage(params)
	if err != nil {
		log.Error().Msgf("an error occurred while sending sms,"+
			" retry will be attempted... Error: %s", err.Error())
		return &Error{
			ErrorType: RECOVERABLE,
		}
	}
	log.Info().Msgf("Sms to %s sent successfully. SID: %s", notification.TelephoneNumber, *resp.Sid)

	return nil
}

func (s *SMSHandler) getSmsParams(data *SMSNotification) *api.CreateMessageParams {
	params := &api.CreateMessageParams{}
	params.SetBody(data.Message)
	params.SetFrom(s.virtualAppTelephoneNumber)
	params.SetTo(data.TelephoneNumber)

	return params
}
