package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dadosjusbr/storage"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"go.mongodb.org/mongo-driver/bson"
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
	aid   = flag.String("aid", "", "Órgão")
	year  = flag.Int("year", 2018, "Ano")
	month = flag.Int("month", 1, "Mês")
)

func main() {
	addRev()
	flag.Parse()

	f, err := os.OpenFile("out.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

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

	if mi.ProcInfo != nil {
		str := fmt.Sprintf("MONTHS=%d YEAR=%d AID=%s ./exec.sh\n", *month, *year, *aid)
		_, err := f.WriteString(str)

		if err != nil {
			log.Fatal(err)
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

func addRev() {
	flag.Parse()

	f, err := os.OpenFile("out.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	client, err := mongo.Connect(ctx)
	if err != nil {
		fmt.Println("mongo.Connect() ERROR:", err)
		os.Exit(1)
	}
	defer cancel()

	col := client.Database(c.DBName).Collection(c.MongoMICol)
	filter := bson.M{
		"aid":   *aid,
		"month": *month,
		"year":  *year,
	}

	res, err := col.Find(ctx, filter)
	if err != nil {
		log.Fatal("Erro ao consultar informações mensais dos órgãos: ", err)
	}
	defer res.Close(ctx)
	var agmi storage.AgencyMonthlyInfo
	res.Decode(&agmi)

	// ## Armazenando revisão.
	colRev := client.Database(c.DBName).Collection(c.MongoRevCol)
	rev := storage.MonthlyInfoVersion{
		AgencyID:  agmi.AgencyID,
		Month:     agmi.Month,
		Year:      agmi.Year,
		VersionID: time.Now().Unix(),
		Version:   agmi,
	}
	if _, err := colRev.InsertOne(context.TODO(), rev); err != nil {
		log.Fatal("error trying to insert monthly info revision with value:", err.Error())
	}
}
