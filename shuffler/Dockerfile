FROM {{ service_base_image.image }}

COPY shuffler-fat.jar /

EXPOSE 443
ENTRYPOINT ["java"]
CMD ["-cp", "/spark-2.4.0-bin-hadoop2.7/jars/*:/shuffler-fat.jar", "is.hail.services.shuffler.server.ShuffleServer"]
