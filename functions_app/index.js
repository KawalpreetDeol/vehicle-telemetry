// Testing - input/landing/2023/10/11/Customer_Valid.json
module.exports = async function (context, myBlob) {
    function schemaCheck(datum) {
        if (datum.vehicle_id && datum.latitude && datum.longitude && datum.city && datum.temperature && datum.speed){
            return true
        }
        return true
    }
    function typeCheck(datum) {
        if (typeof(datum.vehicle_id) == "string" && typeof(datum.latitude) == "number" && typeof(datum.longitude) == "number"
        && typeof(datum.temperature) == "number" && typeof(datum.speed) == "number" && typeof(datum.city) == "string"){
            return true
        }
        return false
    }
    function processJSON(data) {
        valid_data = []
        invalid_data = []
        data.forEach(datum => {
            if (datum.temeprature) {
                datum.temperature = datum.temperature ?? datum.temeprature
                delete datum.temeprature
            }
            if (datum.VehicleID) {
                datum.vehicle_id = datum.vehicle_id ?? datum.VehicleID
                delete datum.VehicleID
            }
            if (datum.latitiude) {
                datum.latitude = datum.latitude ?? datum.latitiude
                delete datum.latitiude
            }
            if (datum.City) {
                datum.city = datum.city ?? datum.City
                delete datum.City
            }
        });
        data.forEach(datum => {
            if (!schemaCheck(datum)) {
                invalid_data.push(datum)
            }
            else if (!typeCheck(datum)) {
                invalid_data.push(datum)
            }
            else {
                valid_data.push(datum)
            }
        });
        return [valid_data, invalid_data]
    }
    context.log("JavaScript blob trigger function processed blob \n Blob:");
    context.log("********Azure Function Started********");
    var result = true;
    try{
        // context.log(myBlob.toString());
        var data = JSON.parse(myBlob.toString().trim().replace('\n', ' '));
    }catch(exception){
        context.log(exception);
        result = false;
    }
    try {
        if(result){
            context.log("********Row Validation Started********");
            var ret = processJSON(data)
            context.log(`********Rows Validated Successfully - Accepted: ${ret[0].length} rows, Rejected: ${ret[1].length} rows********`);
            context.bindings.stagingFolder = JSON.stringify(ret[0]); 
            context.bindings.acceptedFolder = JSON.stringify(ret[0])
            context.log("********Accepted Rows File Copied to Staging Folder Successfully********");
            context.bindings.rejectedFolder = JSON.stringify(ret[1]);
            context.log("********Rejected Rows File Copied to Rejected Folder Successfully********");
        } else{
            
            context.bindings.rejectedFolder = myBlob.toString();
            context.log("********Inavlid JSON File Copied to Rejected Folder Successfully********");
        }
    } catch(exception) {
        context.log(exception)
    }
    

    context.log("*******Azure Function Ended Successfully*******");
    
};