package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/dadosjusbr/storage"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"go.mongodb.org/mongo-driver/mongo"
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

	if *aid == "" {
		log.Fatal("Flag aid obrigatória")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)

	client, err := mongo.Connect(ctx)
	if err != nil {
		log.Fatal("mongo.Connect() ERROR:", err)
	}
	defer cancel()

	clientDB, err := newClient(c)
	if err != nil {
		log.Fatalf("Erro ao criar cliente no banco de dados: %v", err)
	}

	for year := 2018; year <= 2021; year++ {
		var operations []mongo.WriteModel

		for month := 1; month <= 12; month++ {
			agmi, _, err := clientDB.GetOMA(month, year, *aid)
			if err != nil {
				log.Fatalf("Erro ao consultar informações mensais do órgão: %v", err)
			}

			// ## Armazenando revisão.
			if agmi.ProcInfo == nil {
				fmt.Printf("%d/%d não ocorreu erro na coleta do %s\n", agmi.Month, agmi.Year, agmi.AgencyID)
				continue
			}
			rev := storage.MonthlyInfoVersion{
				AgencyID:  agmi.AgencyID,
				Month:     agmi.Month,
				Year:      agmi.Year,
				VersionID: time.Now().Unix(),
				Version:   *agmi,
			}
			operation := mongo.NewInsertOneModel().SetDocument(rev)
			operations = append(operations, operation)
		}
		if len(operations) > 0 {
			colRev := client.Database(c.DBName).Collection(c.MongoRevCol)
			results, err := colRev.BulkWrite(ctx, operations)
			if err != nil {
				log.Fatalf("Erro ao inserir em miRev [%s/%d]: %v", *aid, year, err)
			}
			fmt.Printf("Documentos inseridos: %d\n\n", results.ModifiedCount)
		} else {
			fmt.Print("Não há documentos para inserir.\n\n")
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
