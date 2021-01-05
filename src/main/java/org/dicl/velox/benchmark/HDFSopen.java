package org.dicl.velox.benchmark;

import org.apache.hadoop.conf.Configuration;
// org.apache.hadoop.fs 패키지로 HDFS 파일을 제어할 수 있다.
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class HDFSopen 
{
	public static void main(String[] args)
	{
		if(args.length != 1)
		{
			System.err.println(" HDFSopen <filename>");
			System.exit(2);
		}
		try{
			// 파일 시스템 객체를 생성한다.
			Configuration conf = new Configuration(); // 하둡 환경 설정 파일에 접근하기 위한 클래스이다.
			// Configuration 객체가 사용하는 HDFS를 반환한다.
			FileSystem hdfs = FileSystem.get(conf); // 이 클래스를 이용해서 로컬파일 시스템이나 HDFS를 제어할 수 있다.
			// 경로 객체 생성.
			Path path = new Path(args[0]);
			
			// 파일 출력
			FSDataInputStream inputStream = hdfs.open(path);
			inputStream.close();



		}catch(Exception ex){

			ex.printStackTrace();

		}

	}

}
