import org.apache.nifi.flowfile.FlowFile

import java.nio.charset.StandardCharsets
import groovy.json.JsonBuilder
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.usermodel.WorkbookFactory
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.OutputStreamCallback


import java.text.SimpleDateFormat

def flowFile = session.get()
if(!flowFile) return
List<FlowFile> flowFiles = new ArrayList<>()
String absolutePath = flowFile.getAttribute('absolute.path');
String fileName = flowFile.getAttribute('filename');
String filePath = new StringBuilder(absolutePath).append(fileName)
log.warn(filePath)
try {
    def fh = new File(filePath)
    Workbook wb = WorkbookFactory.create(fh,);
    //Workbook wb = WorkbookFactory.create(inputStream);
    log.info("reading workbook")
    /*Sheet mySheet = wb.getSheetAt(0);
fh = new File('/Users/abhayphougat/Documents/TataDigital/UniversalCart/Pincode_LeadTime_upload_01-12-2020.xlsx')
Workbook wb = WorkbookFactory.create(fh,);*/
    wb.each {Sheet mySheet ->
        log.warn("reading sheet")
        List<String> fields = new ArrayList<>()
        def header = mySheet.getRow(0);
        Iterator<Cell> columnNames = header.cellIterator();
        while(columnNames.hasNext())
        {
            def name = columnNames.next().toString().trim();
            fields.add(name);
        }
        FlowFile newFlowFile = session.create(flowFile)
        /*def date = new Date()
        def filename = flowFile.getAttribute('filename').split(".")[0] + '_' + new SimpleDateFormat("YYYYMMdd-HHmmss").format(date)+'.json'*/
        newFlowFile = session.write(newFlowFile, { outputStream ->
        mySheet.rowIterator().eachWithIndex{ Row nextRow, int i ->
            if(i  > 0)
            {

                    List<Map<String, Object>> shipNoteList = new ArrayList<>();
                    Map<String, Object> record = new TreeMap<>();
                    nextRow.cellIterator().forEachRemaining {
                        Cell cell ->
                            def val = cell.toString();
                            def columnName = fields.get(cell.columnIndex);
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
                    outputStream.write(outJson.toString().getBytes(StandardCharsets.UTF_8))
                }
            }
        } as OutputStreamCallback)
        //newFlowFile = session.putAttribute(newFlowFile, 'filename', filename)
        flowFiles.add(newFlowFile)


    }
}
catch(e) {
    log.error("Error during processing of spreadsheet name = xx, sheet = xx", e)
    //session.transfer(inputStream, REL_FAILURE)
}
session.transfer(flowFiles, REL_SUCCESS)

void updateRecord(Map<String, Object> record,String key, Object value){
    record.put(key, value);
}
