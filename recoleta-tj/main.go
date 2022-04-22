package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/dadosjusbr/storage"
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

var (
	aid = flag.String("aid", "", "Órgão")
)

func main() {
	flag.Parse()

	if err := godotenv.Load(".env"); err != nil {
		log.Fatalf("Erro ao carregar arquivo .env: %v", err)
	}

	var c config
	if err := envconfig.Process("", &c); err != nil {
		log.Fatalf("Erro ao carregar parâmetros do arquivo .env:%v", err)
	}

	f, err := os.OpenFile("out.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if *aid == "" {
		log.Fatal("Flag aid obrigatória")
	}

	client, err := newClient(c)
	if err != nil {
		log.Fatalf("Erro ao criar cliente no banco de dados: %v", err)
	}
	for year := 2018; year <= 2021; year++ {
		var monthsErr []int
		for month := 1; month <= 12; month++ {
			filter := map[string]int{
				"month": month,
				"year":  year,
			}
			mi, _, err := client.GetOMA(filter["month"], filter["year"], *aid)
			if err != nil {
				log.Fatalf("Erro ao consultar informações mensais do órgão: %v", err)
			}
			if mi.ProcInfo != nil && mi.ProcInfo.Status == 4 {
				monthsErr = append(monthsErr, month)
			}
		}
		if len(monthsErr) > 0 {
			semiformat := fmt.Sprintf("%d", monthsErr)
			semiformat = strings.Replace(semiformat, "[", `"`, 1)
			monthsList := strings.Replace(semiformat, "]", `"`, 1)
			str := fmt.Sprintf(`MONTHS=%s YEAR=%d AID=%s ./exec.sh%s`, monthsList, year, *aid, "\n")
			_, err := f.WriteString(str)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
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
