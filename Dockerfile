FROM public.ecr.aws/ubuntu/ubuntu:22.04_stable

RUN apt update -y

RUN apt install openjdk-17-jdk -y

RUN apt install maven -y

COPY . ./dbeam/

WORKDIR ./dbeam/

RUN mvn clean package -Ppack

RUN mkdir output_dir

ENTRYPOINT ["java", "-cp", "./dbeam-core/target/dbeam-core-shaded.jar", "com.spotify.dbeam.jobs.JdbcAvroJob", "--output=./output_dir"]
