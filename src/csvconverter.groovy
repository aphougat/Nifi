import groovy.json.JsonBuilder
import org.apache.nifi.processor.io.StreamCallback

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

def flowFile = session.get()
if(!flowFile) return

flowFile = session.write(flowFile, { inputStream, outputStream ->
    //log.warn("reading csv")
    List<String> fields = new ArrayList<>()
    try {
        inputStream.eachLine { line, lineNumber ->
            if (lineNumber == 1) {
                fields = line.split(';');
                //log.warn("fields getting populated")
                //log.warn(fields.toString())
            } else {

                def vals = line.split(';');
                //log.warn("values getting populated")
                //log.warn(vals.toString())
                List<Map<String, Object>> shipNoteList = new ArrayList<>();
                Map<String, Object> record = new TreeMap<>();
                vals.eachWithIndex {
                    String cell, index ->
                        def val = cell;
                        def columnName = fields.get(index);
                        if (columnName.equalsIgnoreCase("Shipnode")) {
                            Map<String, Object> storePriority = new TreeMap<>();
                            storePriority.put("name", val);
                            storePriority.put("id", val);
                            shipNoteList.add(storePriority);
                            updateRecord(record, columnName, shipNoteList);
                        } else if (columnName.equalsIgnoreCase("Leadtime") || columnName.equalsIgnoreCase("Channel") || columnName.equalsIgnoreCase("Carrier")) {
                            shipNoteList.last().put(columnName, val);
                        } else {
                            updateRecord(record, columnName, val);
                        }

                }
                def outJson = new JsonBuilder(record);
                //log.warn(outJson.toString());
                outputStream.write(outJson.toString().getBytes(StandardCharsets.UTF_8))

            }

        }

    }
    catch (e) {
        log.error("Error during processing of spreadsheet name = xx, sheet = xx", e)
        //session.transfer(inputStream, REL_FAILURE)
    }
} as StreamCallback)
def date = new Date()
def filename = flowFile.getAttribute('filename').split("\\.")[0] + '_' + new SimpleDateFormat("YYYYMMdd-HHmmss").format(date)+'.json'
flowFile = session.putAttribute(flowFile, 'filename', filename)
session.transfer(flowFile, REL_SUCCESS)

void updateRecord(Map<String, Object> record,String key, Object value){
    record.put(key, value);
}
