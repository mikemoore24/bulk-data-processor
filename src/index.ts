// Kick off scripts in psc and company data to write to new file.
import {transformPSCData, convertJSONToCSV} from "./file-transform-psc";
import {processCompanyData} from "./file-transform-company";

processCompanyData();

// Can run this on its own if the PSC JSON data has already been done.
// convertJSONToCSV();

function notifyNextStep() {
    transformPSCData();
}

export {notifyNextStep};
