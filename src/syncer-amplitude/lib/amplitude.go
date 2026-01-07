package amplitude

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	AMPLITUDE_API_URL     = "https://amplitude.com/api/2/export"
	AMPLITUDE_TIME_FORMAT = "20060102T15"
)

type Amplitude struct {
	Config     *Config
	HttpClient *http.Client
}

func NewAmplitude(config *Config) *Amplitude {
	return &Amplitude{
		Config:     config,
		HttpClient: &http.Client{Timeout: 5 * time.Minute},
	}
}

func (amplitude *Amplitude) Export(jsonQueueWriter *common.JsonQueueWriter, startTime, endTime time.Time) error {
	startString := startTime.UTC().Format(AMPLITUDE_TIME_FORMAT)
	endString := endTime.UTC().Format(AMPLITUDE_TIME_FORMAT)

	req, err := http.NewRequest("GET", AMPLITUDE_API_URL, nil)
	if err != nil {
		return err
	}
	q := req.URL.Query()
	q.Add("start", startString)
	q.Add("end", endString)
	req.URL.RawQuery = q.Encode()
	req.SetBasicAuth(amplitude.Config.ApiKey, amplitude.Config.SecretKey)

	common.LogInfo(amplitude.Config.CommonConfig, "Fetching data from Amplitude from", startString, "to", endString)
	resp, err := amplitude.HttpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("amplitude API returned status %d: %s", resp.StatusCode, string(body))
	}
	common.LogDebug(amplitude.Config.CommonConfig, "Received response from Amplitude:", resp.Status)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return err
	}

	for _, zipFile := range zipReader.File {
		common.LogInfo(amplitude.Config.CommonConfig, "Processing file:", zipFile.Name)
		unzippedFile, err := zipFile.Open()
		if err != nil {
			return err
		}
		defer unzippedFile.Close()

		gzipReader, err := gzip.NewReader(unzippedFile)
		if err != nil {
			return err
		}
		defer gzipReader.Close()

		decoder := json.NewDecoder(gzipReader)
		for {
			var event RecordEvent
			err := decoder.Decode(&event)
			if err != nil {
				if err == io.EOF {
					break // We're done
				}
				return err
			}

			err = jsonQueueWriter.Write(event.ToMap())
			if err != nil {
				return err
			}
		}
	}

	return nil
}
