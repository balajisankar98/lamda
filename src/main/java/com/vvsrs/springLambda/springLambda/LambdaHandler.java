package com.vvsrs.springLambda.springLambda;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.cloud.function.adapter.aws.SpringBootRequestHandler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LambdaHandler extends SpringBootRequestHandler<S3Event,String> {
	
	private static final AmazonS3 s3Client;
	
	private static final String putBucket = "idc-data";
	
	static {
		s3Client = AmazonS3ClientBuilder.defaultClient();
	}
	
	@Override
	public Object handleRequest(S3Event event, Context context) {
		log.info("Entering functional interface apply function");
        S3EventNotification.S3EventNotificationRecord record = event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();
        log.info("SOURCE BUCKET: " + srcBucket);
        log.info("SOURCE KEY: " + srcKey);
        List<String> ids = getS3Object.andThen(getIds).apply(srcBucket,srcKey);
        if(ids == null || ids.isEmpty()){
        	log.error("Error in getting ids");
        	return null;
		}
        byte[] data = getDataFromIDC.andThen(generateExcel).apply(ids);
        if(data == null || data.length == 0) {
        	log.error("Error in generating excel");
        	return null;
		}
        saveExcelToS3.accept(data);
        log.info("Completed");
		return null;
	}
	
	Consumer<byte[]> saveExcelToS3 = data->{
		if(!s3Client.doesBucketExistV2(putBucket)) {
			s3Client.createBucket(putBucket);
		}
		
		s3Client.putObject(putBucket, 
				LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss"))+".xls",
				new ByteArrayInputStream(data),
				new ObjectMetadata());
	};

	public static void main(String[] args) {
		System.out.println(LocalDate.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy")));
	}
	
	BiFunction<String,String,S3Object> getS3Object = (srcBucket,srcKey)->{
        return s3Client.getObject(srcBucket, srcKey);
	};
	
	Function<S3Object,List<String>> getIds = s3Object->{
		if(s3Object == null) {
			log.error("CSV not present in the bucket");
			return null;
		}
		 Reader in = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
		 try(CSVParser csvParser = new CSVParser(in,CSVFormat.DEFAULT)) {
			List<String> ids = new ArrayList<>();
			csvParser.forEach(eachRow->{
				ids.add(eachRow.get(0));
			});
			log.info("IDS - {}",ids);
			return ids;
		} catch (IOException e) {
			log.error("Error at extracting ids from s3Obect");
			log.error(e.getMessage());
		}
		 return null;
	};
	
	Function<List<String>,List<Map<String,String>>> getDataFromIDC = ids->{
		return ids.stream().map(id->{
			Map<String,String> map = new HashMap<>();
			map.put("id",id);
			map.put("field1","field1"+id);
			map.put("field2","field2"+id);
			return map;
		}).collect(Collectors.toList());
	};
	
	Function<List<Map<String,String>>,byte[]> generateExcel = idcData->{
		if(idcData!= null && !idcData.isEmpty()) {
			WritableWorkbook  workBook = null;
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()){
				workBook = Workbook.createWorkbook(baos);
	            // create an Excel sheet
				 constructExcel(idcData, workBook);
	            workBook.write();
	            workBook.close();
	            workBook = null;
	            return baos.toByteArray();
	        } catch (IOException | WriteException e) {
	        	log.error(e.getMessage());
	        } finally {
	            if (workBook != null) {
	                try {
	                    workBook.close();
	                }catch (IOException | WriteException e) {
	                	log.error(e.getMessage());
	    	        }
	            }
	        }

		}
		return null;
	};
	
	
	private void constructExcel(List<Map<String, String>> idcData, WritableWorkbook workBook)
			throws WriteException {
		WritableSheet excelSheet = null;
		 int sheetNo = 0;
		 int rowNum = 0;
		 int col = 0;
		 Set<String> headers = idcData.get(0).keySet();
		for(int i = 0;i < idcData.size();i++)
		{
			Map<String, String> log = idcData.get(i);
			if(i == 0 || i % 59999 == 0)
		    {
				excelSheet = workBook.createSheet("Sheet "+(sheetNo+1),sheetNo);
				sheetNo++;
				rowNum = 0;
				col = 0;
				for(String header:headers){
					 Label label = new Label(col,rowNum, header);
					 excelSheet.addCell(label);
					 col++;
				}
				rowNum++;
		    }
			col = 0;
			for(String header:headers){
				String value = log.get(header);
				 Label label = new Label(col,rowNum,value);
				 excelSheet.addCell(label);
				 col++;
			}
			rowNum++;
		}
	}
}