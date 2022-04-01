package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dadosjusbr/datapackage"
	"github.com/dadosjusbr/storage"
	flData "github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/tableschema-go/csv"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	MongoURI    string `envconfig:"MONGODB_URI" required:"true"`
	DBName      string `envconfig:"MONGODB_DBNAME" required:"true"`
	MongoMICol  string `envconfig:"MONGODB_MICOL" required:"true"`
	MongoAgCol  string `envconfig:"MONGODB_AGCOL" required:"true"`
	MongoPkgCol string `envconfig:"MONGODB_PKGCOL" required:"true"`
	MongoRevCol string `envconfig:"MONGODB_REVCOL" required:"true"`

	// Swift Conf
	SwiftUsername  string `envconfig:"SWIFT_USERNAME" required:"true"`
	SwiftAPIKey    string `envconfig:"SWIFT_APIKEY" required:"true"`
	SwiftAuthURL   string `envconfig:"SWIFT_AUTHURL" required:"true"`
	SwiftDomain    string `envconfig:"SWIFT_DOMAIN" required:"true"`
	SwiftContainer string `envconfig:"SWIFT_CONTAINER" required:"true"`
}

const (
	coletaResourceName       = "coleta"        // hardcoded in datapackage_descriptor.json
	contrachequeResourceName = "contra_cheque" // hardcoded in datapackage_descriptor.json
	remuneracaoResourceName  = "remuneracao"   // hardcoded in datapackage_descriptor.json
	metadadosResourceName    = "metadados"     // hardcoded in datapackage_descriptor.json
)

var (
	aid   = flag.String("aid", "", "Órgão")
	year  = flag.Int("year", 2018, "Ano")
	month = flag.Int("month", 1, "Mês")
)

func main() {
	flag.Parse()
	downloadsFolder := filepath.Join(".", "downloads")
	if err := os.MkdirAll(downloadsFolder, os.ModePerm); err != nil {
		log.Fatal("Erro criando diretório downloads: ", err)
	}

	outputFolder := filepath.Join(".", "generated")
	if err := os.MkdirAll(outputFolder, os.ModePerm); err != nil {
		log.Fatal("Erro criando diretório downloads: ", err)
	}

	if *aid == "" {
		log.Fatal("Flag aid obrigatória")
	}

	if err := godotenv.Load(".env"); err != nil {
		log.Fatal("Erro ao carregar arquivo .env.")
	}

	var c config
	if err := envconfig.Process("", &c); err != nil {
		log.Fatal("Erro ao carregar parâmetros do arquivo .env: ", err.Error())
	}

	client, err := newClient(c)
	if err != nil {
		log.Fatal("Erro ao criar cliente no banco de dados: ", err.Error())
	}

	mi, _, err := client.GetOMA(*month, *year, *aid)
	if err != nil {
		log.Fatal("Erro ao consultar informações mensais do órgão: ", err.Error())
	}

	currentTime := time.Now()
	fmt.Printf("%s Atualizando datapackage para [%s] em [%d]/[%d]...\n", currentTime.Format("2006-02-01 15:04:05"), *aid, *month, *year)

	// Quando não houver o dado ou problema na coleta
	if mi.Package == nil {
		fmt.Print("--\n")
		return
	}

	oldZipPath := fmt.Sprintf("./downloads/%s-%d-%d.zip", mi.AgencyID, mi.Month, mi.Year)
	if err := downloadFile(oldZipPath, mi.Package.URL); err != nil {
		log.Fatal("Erro ao baixar zip: %w", err.Error())
	}

	unzip(oldZipPath, "unzipped")

	defer func() {
		os.Remove(oldZipPath)
		os.Remove("./unzipped/coleta.csv")
		os.Remove("./unzipped/contra_cheque.csv")
		os.Remove("./unzipped/metadados.csv")
		os.Remove("./unzipped/remuneracao.csv")
		os.Remove("./unzipped/datapackage.json")
	}()

	rcCSV, err := load(oldZipPath)
	if err != nil {
		log.Fatal("Erro ao obter ResultadoColeta_CSV do Load [", oldZipPath, "]: ", err.Error())
	}

	rcCSV.Metadados[0].IndiceCompletude = float32(mi.Score.CompletenessScore)
	rcCSV.Metadados[0].IndiceFacilidade = float32(mi.Score.EasinessScore)
	rcCSV.Metadados[0].IndiceTransparencia = float32(mi.Score.Score)

	zipName := filepath.Join(outputFolder, fmt.Sprintf("%s-%d-%d.zip", mi.AgencyID, mi.Year, mi.Month))
	if err := datapackage.Zip(zipName, rcCSV, false); err != nil {
		log.Fatal("Erro ao obter novo Zip [", oldZipPath, "]: ", err.Error())
	}

	packBackup, err := client.Cloud.UploadFile(zipName, *aid)
	if err != nil {
		log.Fatal("Erro ao tentar fazer upload do novo zip [", zipName, "]: ", err.Error())
	}

	mi.Package = packBackup
	if err := client.StorePackage(storage.Package{
		AgencyID: aid,
		Year:     year,
		Month:    month,
		Group:    nil,
		Package:  *packBackup}); err != nil {
		log.Fatal("Erro ao tentar fazer StorePackage do novo zip [", zipName, "]: ", err.Error())
	}

	if err := client.Store(*mi); err != nil {
		log.Fatal("Erro ao tentar fazer Store do novo zip [", zipName, "]: ", err.Error())
	}

	fmt.Printf("%s Datapackage atualizado para [%s] em [%d]/[%d]...\n", currentTime.Format("2006-02-01 15:04:05"), *aid, *month, *year)
}

func downloadFile(filepath string, url string) error {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

func unzip(src string, dest string) ([]string, error) {

	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, fmt.Errorf("%s: illegal file path", fpath)
		}

		filenames = append(filenames, fpath)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return filenames, err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return filenames, err
		}

		rc, err := f.Open()
		if err != nil {
			return filenames, err
		}

		_, err = io.Copy(outFile, rc)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()

		if err != nil {
			return filenames, err
		}
	}
	return filenames, nil
}

func load(path string) (datapackage.ResultadoColeta_CSV, error) {
	pkg, err := flData.Load("unzipped/datapackage_fix.json")
	if err != nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("error loading datapackage (%s):%q", path, err)
	}

	coleta := pkg.GetResource(coletaResourceName)
	if coleta == nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("resource coleta not found in package %s", path)
	}
	var coleta_CSV []datapackage.Coleta_CSV
	if err := coleta.Cast(&coleta_CSV, csv.LoadHeaders()); err != nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("failed to cast Coleta_CSV: %s", err)
	}

	contracheque := pkg.GetResource(contrachequeResourceName)
	if contracheque == nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("resource contra_cheque not found in package %s", path)
	}
	var contracheque_CSV []datapackage.ContraCheque_CSV
	if err := contracheque.Cast(&contracheque_CSV, csv.LoadHeaders()); err != nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("failed to cast ContraCheque_CSV: %s", err)
	}

	remuneracao := pkg.GetResource(remuneracaoResourceName)
	if remuneracao == nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("resource remuneracao not found in package %s", path)
	}
	var remuneracao_CSV []datapackage.Remuneracao_CSV
	if err := remuneracao.Cast(&remuneracao_CSV, csv.LoadHeaders()); err != nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("failed to cast Remuneracao_CSV: %s", err)
	}

	metadados := pkg.GetResource(metadadosResourceName)
	if metadados == nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("resource metadados not found in package %s", path)
	}
	var metadados_CSV []datapackage.Metadados_CSV
	if err := metadados.Cast(&metadados_CSV, csv.LoadHeaders()); err != nil {
		return datapackage.ResultadoColeta_CSV{}, fmt.Errorf("failed to cast Metadados_CSV: %s", err)
	}

	return datapackage.ResultadoColeta_CSV{
		Coleta:       coleta_CSV,
		Remuneracoes: remuneracao_CSV,
		Folha:        contracheque_CSV,
		Metadados:    metadados_CSV,
	}, nil
}

// newClient Creates client to connect with DB and Cloud5
func newClient(conf config) (*storage.Client, error) {
	db, err := storage.NewDBClient(conf.MongoURI, conf.DBName, conf.MongoMICol, conf.MongoAgCol, conf.MongoPkgCol, conf.MongoRevCol)
	if err != nil {
		return nil, fmt.Errorf("error creating DB client: %q", err)
	}
	db.Collection(conf.MongoMICol)
	bc := storage.NewCloudClient(conf.SwiftUsername, conf.SwiftAPIKey, conf.SwiftAuthURL, conf.SwiftDomain, conf.SwiftContainer)
	client, err := storage.NewClient(db, bc)
	if err != nil {
		return nil, fmt.Errorf("error creating storage.client: %q", err)
	}
	return client, nil
}
