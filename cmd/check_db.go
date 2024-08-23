/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"database/sql"
	"embed"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"gov.gsa.fac.cgov-util/internal/logging"
	"gov.gsa.fac.cgov-util/internal/vcap"
)

var (
	source_database string
	//go:embed assets/db_tables.txt
	f embed.FS
)

func check_if_table_exists(source_creds vcap.Credentials) {
	// SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'schema_name' AND tablename  = 'table_name');
	db, err := sql.Open("postgres", source_creds.Get("uri").String())
	if err != nil {
		logging.Logger.Println("TABLECHECK could not connect to DB for checking table existance")
		logging.Logger.Printf("DBTOS3 %s\n", err)
		os.Exit(logging.DB_SCHEMA_SCAN_FAILURE)
	}

	// file, err := os.Open("db_tables.txt")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer file.Close()

	file, err := f.ReadFile("assets/db_tables.txt")
	//print(string(file))
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(strings.NewReader(string(file)))
	//scanner := bufio.NewScanner(file)
	var not_existing []string
	for scanner.Scan() {
		//scanner.Text()
		query := fmt.Sprintf("select * from %s LIMIT 1;", scanner.Text())
		//query := fmt.Sprintf("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '%s')", scanner.Text())
		rows, table_check := db.Query(query)

		if table_check == nil {
			//fmt.Println(i + " exists")
			rows.Next()
		} else {
			//logging.Logger.Println(scanner.Text() + " does not exist")
			// store all scanner.Text() into a map
			// if map != nil
			// hard exit
			not_existing = append(not_existing, "Missing Table: "+scanner.Text())
		}
	}
	if len(not_existing) > 0 {
		logging.Error.Println("CHECKTABLESFAIL : A list of tables that does not exist in the database, but does exist in a manifest has been returned.")
		logging.Error.Println("System exiting...")
		joined_tables := strings.Join(not_existing[:], "\n")
		logging.Error.Printf("DBMISSINGTABLES \n" + joined_tables)
		os.Exit(logging.DB_MISSING_TABLES)
	} else {
		logging.Status.Printf("CHECKTABLESPASS : Manifest and Database tables appear to be in sync for database: " + source_database)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

// checkDbCmd represents the checkDb command
var checkDbCmd = &cobra.Command{
	Use:   "check_db",
	Short: "Check the database against the manifest and determine if there are any missing tables.",
	Long:  `Check the database against the manifest and determine if there are any missing tables.`,
	Run: func(cmd *cobra.Command, args []string) {
		db_creds := getDBCredentials(source_database)
		//stringInSlice(table, list_of_tables)
		check_if_table_exists(db_creds)

	},
}

func init() {
	rootCmd.AddCommand(checkDbCmd)
	parseFlags("check_tables", checkDbCmd)
}
