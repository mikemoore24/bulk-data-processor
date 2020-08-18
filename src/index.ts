// Kick off scripts in psc and company data to write to new file.
import {transformPSCData, convertJSONToCSV} from "./file-transform-psc";
import {cleanseCompanyData} from "./file-transform-company";

cleanseCompanyData();

// TODO
// Read Company data, save off a million records and add company Number to DB.
// Read PSC Data and for each PSC with matching CompNo, save to new file.

// Can run this on its own if the PSC JSON data has already been done.
// convertJSONToCSV();

function notifyNextStep() {
    transformPSCData();
}

export {notifyNextStep};
