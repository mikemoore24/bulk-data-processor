import * as fs from "fs";
import * as JSONStream from "JSONStream";
import {removeEscapeCharactersForValue} from "./util";

// To be config'd
const PATH_TO_DATA_ONE = "../../BulkData/pscInput1.json";
const PATH_TO_DATA_TWO = "../../BulkData/pscInput2.json";
const OUTPUT_PATH_ONE = "../../BulkData/PSCOutput1.json";
const OUTPUT_PATH_TWO = "../../BulkData/PSCOutput2.json";
const OUTPUT_PATH_CSV = "../../BulkData/PSCOutputCSV.csv";
let outputPSCArray = [];

function transformPSCData() {

    outputPSCArray = [];
    const readStreamOne = fs.createReadStream(PATH_TO_DATA_ONE);
    const readStreamTwo = fs.createReadStream(PATH_TO_DATA_TWO);
    let parser = JSONStream.parse(readStreamOne);

    readStreamOne.on("end", () => {
        console.log("PSC data read and saved to array");
        console.log(outputPSCArray.length);
        outputDataToFile(OUTPUT_PATH_ONE);
        // kick off second half
        parser = JSONStream.parse(readStreamTwo);
        readStreamTwo.pipe(parser);
        parser.on("data", (obj) => {
            const populatedPSCObject = getNewPSCObject(obj);
            outputPSCArray.push(populatedPSCObject);
        });
    });
    readStreamTwo.on("end", () => {
        console.log("PSC data read and saved to array");
        console.log(outputPSCArray.length);
        outputDataToFile(OUTPUT_PATH_TWO);
        convertJSONToCSV();
    });

    readStreamOne.pipe(parser);

    parser.on("data", (obj) => {
        const populatedPSCObject = getNewPSCObject(obj);
        outputPSCArray.push(populatedPSCObject);
    });

}

function outputDataToFile(outputPath) {
    const writeStream = fs.createWriteStream(outputPath);
    console.log(outputPSCArray[0]);
    for (const pscObj of outputPSCArray) {
        writeStream.write(JSON.stringify(pscObj) + "\r\n");
    }
    writeStream.end();
    outputPSCArray = [];
}

function convertJSONToCSV() {
    const writeStream = fs.createWriteStream(OUTPUT_PATH_CSV);
    writeStream.write("companyNumber,premises,addrLineOne,locality,country,postCode,ceasedDate," +
        "countryOfResidence,dob,name,kind,nationality,notifiedDate,noc,\r\n");
    const firstJSONReadStream = fs.createReadStream(OUTPUT_PATH_ONE);
    let parser = JSONStream.parse(firstJSONReadStream);

    parser.on("data", (obj) => {
        const csvObjectString = getCSVString(obj);
        writeStream.write(csvObjectString);
    });

    firstJSONReadStream.on("end", () => {
        const secondJSONReadStream = fs.createReadStream(OUTPUT_PATH_TWO);
        parser = JSONStream.parse(secondJSONReadStream);
        console.log("Finished converting FIRST JSON records to CSV")
        parser.on("data", (obj) => {
            const csvObjectString = getCSVString(obj);
            writeStream.write(csvObjectString);
        });

        secondJSONReadStream.on("end", () => {
            console.log("Finished converting SECOND JSON records to CSV");
            writeStream.end();
        });

        secondJSONReadStream.pipe(parser);
    });

    firstJSONReadStream.pipe(parser);
}

function getCSVString(obj) {
    let newString = obj.comp_no + "," + removeEscapeCharactersForValue(obj.addr.no) + "," +
        removeEscapeCharactersForValue(obj.addr.addr_ln_1) + "," + removeEscapeCharactersForValue(obj.addr.area) + "," +
        removeEscapeCharactersForValue(obj.addr.cntry) + "," + obj.addr.post_code + "," + obj.ceased_on + "," +
        removeEscapeCharactersForValue(obj.cntry_residence) + "," + obj.dob.month + "/" + obj.dob.year + "," +
        removeEscapeCharactersForValue(obj.name) + "," + obj.kind + "," +
        removeEscapeCharactersForValue(obj.nationality) + "," + obj.notified_on + ",\"";

    for (let i = 0; i < obj.noc.length; i++) {
        if (obj.noc.length - 1 !== i) {
            newString += obj.noc[i] + ",";
        } else {
            newString += obj.noc[i] + "\",\r\n";
        }
    }

    return newString;
}

function getNewPSCObject(pscData) {
    const dobMonth = pscData.data.date_of_birth ? pscData.data.date_of_birth.month ? pscData.data.
        date_of_birth.month : "blank" : "blank";
    const dobYear = pscData.data.date_of_birth ? pscData.data.date_of_birth.year ? pscData.data.
        date_of_birth.year : "blank" : "blank";
    const forename = pscData.data.name_elements ? pscData.data.name_elements.forename ? pscData.data.
        name_elements.forename : "blank" : "blank";
    const middleName = pscData.data.name_elements ? pscData.data.name_elements.middle_name ? pscData.data.
        name_elements.middle_name : "blank" : "blank";
    const surname = pscData.data.name_elements ? pscData.data.name_elements.surname ? pscData.data.
        name_elements.surname : "blank" : "blank";
    const title = pscData.data.name_elements ? pscData.data.name_elements.title ? pscData.data.
        name_elements.title : "blank" : "blank";
    const addressOne = pscData.data.address ? pscData.data.address.address_line_1 ? pscData.data.
        address.address_line_1 : "blank" : "blank";
    const country = pscData.data.address ? pscData.data.address.country ? pscData.data.
        address.country : "blank" : "blank";
    const locality = pscData.data.address ? pscData.data.address.locality ? pscData.data.
        address.locality : "blank" : "blank";
    const postalCode = pscData.data.address ? pscData.data.address.postal_code ? pscData.data.
        address.postal_code : "blank" : "blank";
    const premises = pscData.data.address ? pscData.data.address.premises ? pscData.data.
        address.premises : "blank" : "blank";

    const pscObj = {
        addr: {
            addr_ln_1: addressOne,
            area: locality,
            cntry: country,
            no: premises,
            post_code: postalCode,
        },
        ceased_on: pscData.data.ceased_on ? pscData.data.ceased_on : "unknown",
        cntry_residence: pscData.data.country_of_residence ? pscData.data.country_of_residence : "unknown",
        comp_no: pscData.company_number ? pscData.company_number : "unknown",
        dob: {
            month: dobMonth,
            year: dobYear,
        },
        forename,
        kind: pscData.data.kind ? pscData.data.kind : "unknown",
        mid_name: middleName,
        name: pscData.data.name ? pscData.data.name : "unknown",
        nationality: pscData.data.nationality ? pscData.data.nationality : "unknown",
        noc: pscData.data.natures_of_control ? pscData.data.natures_of_control : "unknown",
        notified_on: pscData.data.notified_on ? pscData.data.notified_on : "unknown",
        surname,
        title,
    };
    return pscObj;
}

export {transformPSCData, convertJSONToCSV};
