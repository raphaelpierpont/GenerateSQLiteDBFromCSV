<?php
// config
$csvDir = __DIR__ . "/csv";  // folder containing CSV files
$dbFile = __DIR__ . "/csv_data.db"; // SQLite file (change to ":memory:" for RAM-only)

// connect to SQLite
$db = new PDO("sqlite:" . $dbFile);
$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// clear previous tables (optional)
$db->exec("PRAGMA foreign_keys = OFF;");
$tables = $db->query("SELECT name FROM sqlite_master WHERE type='table'")->fetchAll(PDO::FETCH_COLUMN);
foreach ($tables as $table) {
    $db->exec("DROP TABLE IF EXISTS " . $table);
}

// process all CSV files in the folder
foreach (glob($csvDir . "/*.csv") as $file) {
    $tableName = pathinfo($file, PATHINFO_FILENAME);

    if (($handle = fopen($file, "r")) !== false) {
        $headers = fgetcsv($handle, 0, ","); // first row = headers

        // sanitize column names
        $columns = array_map(function($col) {
            return preg_replace('/[^a-zA-Z0-9_]/', '_', strtolower($col));
        }, $headers);

        // create table
        $colDefs = array_map(fn($col) => "`$col` TEXT", $columns);
        $db->exec("CREATE TABLE IF NOT EXISTS `$tableName` (" . implode(", ", $colDefs) . ")");

        // prepare insert statement
        $placeholders = implode(", ", array_fill(0, count($columns), "?"));
        //echo "INSERT INTO `$tableName` (`" . implode("`, `", $columns) . "`) VALUES ($placeholders)\n";
        $stmt = $db->prepare("INSERT INTO `$tableName` (`" . implode("`, `", $columns) . "`) VALUES ($placeholders)");

        // insert rows
        $db->beginTransaction();

        while (($row = fgetcsv($handle, 0, ",")) !== false) {
            $stmt->execute($row);
        }
        
        $db->commit();
        
        fclose($handle);
        echo "Loaded: $tableName (" . count($columns) . " columns)\n";
    }
}

echo "All CSVs loaded into SQLite.\n";
?>
