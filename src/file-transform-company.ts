import * as fs from "fs";
import * as csv from "csv-parser";
import {notifyNextStep} from "./index";
import {removeEscapeCharactersForValue} from "./util";

// To be config"d
// Change offset value to begin saving records at that marker.
const offsetRecords = 1500000;
const maxRecords = 1000000;
const PATH_TO_DATA = "../../BulkData/BasicCompanyDataAsOneFile20200501.csv";
const OUTPUT_PATH = "../../BulkData/CompanyOutput1MaxRecords1500000.csv";
const companyNumbersArr = [];

let currentCompanyCount = 0;

function processCompanyData() {
    const headersStr = "compName,compNo,compCategory,compStatus,addrLn1,addrLn2,town,county,country,postcode,countryOrigin,"
        + "dissolutionDate,incDate,acctsLastDate,confStmtLastDate,prevNameDate1,prevName1,prevNameDate2,"
        + "prevName2,prevNameDate3,prevName3,prevNameDate4,prevName4,prevNameDate5,prevName5,\r\n";

    const writer = fs.createWriteStream(OUTPUT_PATH);
    writer.write(headersStr);
    writer.end();

    const writerAppend = fs.createWriteStream(OUTPUT_PATH, {flags: "a"});
    // Read CSV, parse through getNewCompanyString and outputDataToFile
    // NOTE - REMOVE EXISTING FILE BEFORE RE-PROCESSING
    fs.createReadStream(PATH_TO_DATA)
        .pipe(csv())
        .on("data", (row) => {
            // Don't take any records before the offset and only enter condition if within maxRecord value
            currentCompanyCount++;
            if (currentCompanyCount > offsetRecords && currentCompanyCount < (maxRecords + offsetRecords)) {
                const newObj = fillGapsInData(row);
                // Store off company number to save corresponding PSCs later
                const regExp = new RegExp('"', "g");
                const compNumber = newObj["CompanyNumber"].replace(regExp, "");
                companyNumbersArr.push(compNumber);
                // console.log(row);
                writerAppend.write(getCompanyObjectString(newObj));
            }
        })
        .on("end", () => {
            writerAppend.end();
            console.log("CSV file successfully processed");
            notifyNextStep();
        });
}

function fillGapsInData(row) {
    const newObj = {};
    // Process the row and return with no empty strings
    Object.keys(row).forEach((key) => {
        if (row[key] === "") {
            row[key] = "blank";
        }
        row[key] = removeEscapeCharactersForValue(row[key]);
        newObj[key.replace(".","").replace(" ", "")] = row[key];
    });
    return newObj;
}

function getCompanyObjectString(row) {
    // console.log("row[\"PreviousName_1CompanyName\"]", row. PreviousName_1CompanyName);
    return row.CompanyName + "," + row.CompanyNumber + "," +
        row.CompanyCategory + "," + row.CompanyStatus  + "," +
        row.RegAddressAddressLine1 + "," +
        row.RegAddressAddressLine2 + "," +
        row.RegAddressPostTown + "," + row.RegAddressCounty + "," + row.RegAddressCountry + "," +
        row.RegAddressPostCode + "," + row.CountryOfOrigin + "," + row.DissolutionDate + "," +
        row.IncorporationDate + "," +
        row.AccountsLastMadeUpDate + "," + row.ConfStmtLastMadeUpDate + "," + row.PreviousName_1CONDATE + "," +
        row.PreviousName_1CompanyName + "," + row.PreviousName_2CONDATE + "," +
        row.PreviousName_2CompanyName + "," + row.PreviousName_3CONDATE + "," +
        row.PreviousName_3CompanyName + "," + row.PreviousName_4CONDATE + "," +
        row.PreviousName_4CompanyName + "," + row.PreviousName_5CONDATE + "," +
        row.PreviousName_5CompanyName + ",\r\n";
}

export {processCompanyData, companyNumbersArr};
