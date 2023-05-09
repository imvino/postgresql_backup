const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

const backupFolder = path.join(__dirname, 'backup');
if (!fs.existsSync(backupFolder)) {
    fs.mkdirSync(backupFolder);
}

/*--------------Modify the data--------------*/
const windows_env = false; // for ubuntu false
const startDate = new Date('2022-02-21');
const endDate = new Date('2022-02-27');
const containerName = 'timingplans-db';
const maxBuffer = 1024 * 1024 * 50; //  buffer size 50 MB, adjust according to ram size
const DB = {
    username: 'timingplans',
    password: 'timingplans',
    database: 'timingplans',
    port: 5432
};
/*-------------------------------------------*/

const statusFile = path.join(__dirname, 'download_status.json');
const tablePrefixes = [
    'detection_data',
    'detection_occupancy_data',
    'light_state_data',
    'pedestrian_tmc',
    'tmc',
];

function getDateSuffix(date) {
    return date.getTime();
}

function pgDump(table, outputFile, onSuccess, retryCount = 0) {
    const maxRetries = 3;
    const exeCommand = `docker exec -i ${containerName} pg_dump --host localhost --port ${DB.port} --username ${DB.username} --format plain --verbose --table ${table} ${DB.database}`;
    const command = windows_env ? `PowerShell -Command "$env:PGPASSWORD='${DB.password}'; ${exeCommand}"`
        : `export PGPASSWORD='${DB.password}' && ${exeCommand}`;

    exec(command, { maxBuffer: maxBuffer }, (error, stdout, stderr) => {
        if (error) {
            console.error(`Error while dumping table ${table}: ${error.message}`);
            if (retryCount < maxRetries) {
                console.log(`Retrying... Attempt ${retryCount + 1}`);
                pgDump(table, outputFile, onSuccess, retryCount + 1);
            } else {
                console.error(`Backup failed for table ${table} after ${maxRetries} attempts.`);
                processNextTable(); // Call processNextTable() to continue with the next table in the list
            }
        } else {
            fs.writeFileSync(outputFile, stdout);
            console.log(`Table ${table} dumped successfully.`);
            onSuccess(table);
        }
    });
}


function readStatusFile() {
    try {
        const data = fs.readFileSync(statusFile, 'utf8');
        return JSON.parse(data);
    } catch (error) {
        return {};
    }
}

function updateStatusFile(data) {
    fs.writeFileSync(statusFile, JSON.stringify(data, null, 2));
}

function getProgressPercentage() {
    const totalTables = tablePrefixes.length * ((endDate - startDate) / (24 * 60 * 60 * 1000) + 1);
    const completedTables = Object.keys(dumpStatus).length;
    return (completedTables / totalTables) * 100;
}

let currentDate = new Date(startDate);
let currentPrefixIndex = 0;
const dumpStatus = readStatusFile();

function processNextTable() {
    if (currentPrefixIndex < tablePrefixes.length) {
        const suffix = getDateSuffix(currentDate);
        const prefix = tablePrefixes[currentPrefixIndex];
        const table = `${prefix}_${suffix}`;
        const outputFile = path.join(backupFolder, `${table}.sql`);

        if (!dumpStatus[table]) {
            pgDump(
                table,
                outputFile,
                (table) => {
                    dumpStatus[table] = true;
                    updateStatusFile(dumpStatus);
                    currentPrefixIndex++;
                    processNextTable();
                }
            );
        } else {
            currentPrefixIndex++;
            processNextTable();
        }
    } else {
        currentPrefixIndex = 0;
        currentDate.setDate(currentDate.getDate() + 1);
        if (currentDate <= endDate) {
            processNextTable();
        }
    }
    console.log(`Progress: ${getProgressPercentage().toFixed(2)}%`);
}

processNextTable();
