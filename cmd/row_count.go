/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"gov.gsa.fac.cgov-util/internal/logging"
	"gov.gsa.fac.cgov-util/internal/vcap"
)

var (
	row_count_db string
)

func check_rows_in_db(source_creds vcap.Credentials) {
	db, err := sql.Open("postgres", source_creds.Get("uri").String())
	if err != nil {
		logging.Logger.Println("TABLECHECK could not connect to DB for checking table existance")
		logging.Logger.Printf("DBTOS3 %s\n", err)
		os.Exit(logging.DB_SCHEMA_SCAN_FAILURE)
	}
	file, err := f.ReadFile("assets/db_tables.txt")
	//print(string(file))
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(strings.NewReader(string(file)))
	//var row_count_for_tables []string
	var count int
	for scanner.Scan() {
		//scanner.Text()
		//query := fmt.Sprintf("SELECT count(*) FROM %s;", scanner.Text())
		db.QueryRow("SELECT count(*) FROM %s", scanner.Text()).Scan(&count)
		//rows, row_count := db.Query(query)

		//logging.Status.Printf("Row count for table %s: %s", scanner.Text(), row_count)
		//db.QueryRow("SELECT count(*) FROM %s", scanner.Text()).Scan(&counter)
		//fmt.Sprintf("%s%s/%s-%s.dump", s3path.Bucket, s3path.Key, schema, table)
		logging.Logger.Printf("Table: %s | Row Count: %d", scanner.Text(), count)
		//"Table: %s", counter, "rows"

		//row_count_for_tables = append(row_count_for_tables, scanner.Text(), row_count)
		//rows.Close()
	}
	// logging.Logger.Println("Row count for tables in manifest...")
	// joined_tables := strings.Join(row_count_for_tables[:], " ")
	// logging.Logger.Printf("TABLEROWCOUNT " + joined_tables)
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

// rowCountCmd represents the rowCount command
var rowCountCmd = &cobra.Command{
	Use:   "row_count",
	Short: "Check the rows in a given table",
	Long:  `Check the rows in a given table`,
	Run: func(cmd *cobra.Command, args []string) {
		db_creds := getDBCredentials(row_count_db)
		check_rows_in_db(db_creds)
	},
}

func init() {
	rootCmd.AddCommand(rowCountCmd)
	parseFlags("row_count", rowCountCmd)
}
