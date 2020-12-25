import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback

import java.nio.charset.*
import java.text.SimpleDateFormat

import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import org.apache.poi.ss.usermodel.WorkbookFactory

def flowFile = session.get()
if(!flowFile) return

def date = new Date()
flowFile = session.write(flowFile, {inputStream, outputStream ->
    try {
        String absolutePath = flowFile.getAttribute('absolute.path');
        String fileName = flowFile.getAttribute('filename');
        StringBuilder filePath = new StringBuilder(absolutePath).append(fileName)
        def fh = new File(filePath.toString())
        Workbook wb = WorkbookFactory.create(fh,);
        Sheet mySheet = wb.getSheetAt(0);
        def record = ''

        // processing time, inserted as first column
        def tstamp = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS").format(date)

        Iterator<Row> rowIter = mySheet.rowIterator();
        def rowNum = 0
        while (rowIter.hasNext()) {
            rowNum++
            Row nextRow = rowIter.next();
            Iterator<Cell> cellIterator = nextRow.cellIterator();

            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next()
                record = record + cell.toString() + ','
            }

            if(rowNum > 1){
                // insert tstamp, row num, drop last comma and add end line.
                // Note: tstamp + row num are composite key
                record = tstamp + ',' + rowNum + ',' + record[0..-2] + '\n'
                outputStream.write(record.getBytes(StandardCharsets.UTF_8))
            }
            record = ''
        }

    }
    catch(e) {
        log.error("Error during processing of spreadsheet name = xx, sheet = xx", e)
        //session.transfer(inputStream, REL_FAILURE)
    }
} as StreamCallback)

def filename = flowFile.getAttribute('filename').split('.')[0] + '_' + new SimpleDateFormat("YYYYMMdd-HHmmss").format(date)+'.csv'
flowFile = session.putAttribute(flowFile, 'filename', filename)

session.transfer(flowFile, REL_SUCCESS)