package message

import (
	"context"
	"github.com/jdvr/go-again"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"sumup-sms-notifier/handler"
	"time"
)

type RetryRecoveryRead struct {
	Reader *kafka.Reader
}

type RetryRecoveryWriteToDeadLetterQueue struct {
	Writer  *kafka.Writer
	Message *[]byte
}

func ConsumeRecoveryEvents(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	brokers := viper.GetStringSlice("kafka.bootstrap-servers")
	readerConfig := getKafkaRecoveryConfig(&brokers)
	reader := kafka.NewReader(readerConfig)
	defer func(reader *kafka.Reader) {
		err := reader.Close()
		if err != nil {
			log.Info().Msgf("%s", err.Error())
		}
	}(reader)
	log.Info().Msgf("initialized kafka reader")

	smsHandler := getSmsHandler()

	smsRecoveryWriter := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: viper.GetString("kafka.recovery.topic"),
	}
	defer func(smsRecoveryWriter *kafka.Writer) {
		err := smsRecoveryWriter.Close()
		if err != nil {
			log.Info().Msgf("%s", err.Error())
		}
	}(smsRecoveryWriter)

	readRecoveryMessages(reader, smsRecoveryWriter, smsHandler, ctx, cancel)
}

func readRecoveryMessages(reader *kafka.Reader, writer *kafka.Writer,
	smsHandler *handler.SMSHandler, ctx context.Context,
	cancel context.CancelFunc) {
	backoffConfig := getExponentialBackoffConfig()
	retryService := again.WithExponentialBackoff[kafka.Message](backoffConfig)
	retryOperation := RetryMessageOperation{
		reader,
	}

	log.Info().Msgf("Consuming from the recovery dead letter topic...")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msgf("Retry-Consumer has been canceled.")
			err := reader.Close()
			if err != nil {
				log.Info().Msgf("%s", err.Error())
			}
			err = writer.Close()
			if err != nil {
				log.Info().Msgf("%s", err.Error())
			}
			return
		default:
		}

		msg, err := retryService.Retry(context.Background(), &retryOperation)
		if err != nil {
			log.Error().Msg("Reading from the recovery topic has been unsuccessful, shutting down...")
			cancel()
			continue
		}

		notification, parsingError := smsHandler.ParseData(msg.Value)
		if parsingError != nil {
			log.Error().Msgf("Error parsing message: %v\n", err)
			continue
		}

		recoveryError := smsHandler.SendNotification(notification)
		if isRecoverable(recoveryError) {
			recoveryFallback(ctx, notification, writer, &backoffConfig)
		}

		if recoveryError == nil {
			log.Info().Msgf("SMS recovered successfuly")
		}
	}
}

func getKafkaRecoveryConfig(brokers *[]string) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:       *brokers,
		Topic:         viper.GetString("kafka.recovery.topic"),
		GroupID:       viper.GetString("kafka.recovery.group-id"),
		StartOffset:   kafka.LastOffset,
		RetentionTime: time.Duration(viper.GetInt("kafka.recovery.retention-hours")) * time.Hour,
	}
}

func (operation *RetryRecoveryWriteToDeadLetterQueue) Run(context context.Context) (bool, error) {
	err := operation.Writer.WriteMessages(context, kafka.Message{
		Value: *operation.Message,
	})
	if err != nil {
		log.Info().Msgf("Error reading message: %v\n", err)
		return true, err
	}

	return true, nil
}
