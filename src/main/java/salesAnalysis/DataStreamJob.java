package salesAnalysis;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.http.HttpHost;

import org.elasticsearch.client.Requests;
import salesAnalysis.dto.CategorySalesDTO;
import salesAnalysis.entities.OrderItem;
import salesAnalysis.entities.Product;

import java.util.HashMap;
import java.util.Map;

public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // -------- Kafka Source (new API) --------
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sales-topic")
                .setGroupId("flink-consumer")
                //.setStartingOffsets(OffsetsInitializer.earliest())
                .setStartingOffsets(OffsetsInitializer.latest())   // 🔑 chỉ đọc data mới
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<OrderItem> orderItems = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "KafkaOrderItemsSource"
                )
                .filter(line -> !line.startsWith("OrderItemID"))
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderItem(
                            Integer.parseInt(fields[0]),
                            Integer.parseInt(fields[1]),
                            Integer.parseInt(fields[2]),
                            Integer.parseInt(fields[3]),
                            Float.parseFloat(fields[4])
                    );
                });

        // -------- Products CSV --------
        FileSource<String> productsSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("/Users/phucanhtrannguyen/ApacheFlink-SalesAnalytics/Datasets/products.csv"))
                .build();

        DataStream<Product> products = env.fromSource(productsSource, WatermarkStrategy.noWatermarks(), "ProductsSource")
                .filter(line -> !line.startsWith("ProductID"))
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Product(
                            Integer.parseInt(fields[0]),
                            fields[1],
                            fields[2],
                            Double.parseDouble(fields[3]),
                            fields[4]
                    );
                });

        // -------- Broadcast products --------
        MapStateDescriptor<Integer, Product> productStateDescriptor =
                new MapStateDescriptor<>(
                        "productsBroadcastState",
                        Types.INT,
                        Types.POJO(Product.class)
                );

        BroadcastStream<Product> broadcastProducts = products.broadcast(productStateDescriptor);

        // -------- Join orderItems with products --------
        DataStream<Tuple6<String, String, Float, Integer, Float, String>> joined = orderItems
                .connect(broadcastProducts)
                .process(new ProductBroadcastJoin());

        // -------- Group & Reduce by category --------
        DataStream<CategorySalesDTO> categorySales = joined
                .map((MapFunction<Tuple6<String, String, Float, Integer, Float, String>, CategorySalesDTO>) record ->
                        new CategorySalesDTO(record.f5, record.f4, 1))
                .keyBy(CategorySalesDTO::getCategory)
                .reduce((ReduceFunction<CategorySalesDTO>) (v1, v2) ->
                        new CategorySalesDTO(
                                v1.category,
                                v1.totalSales + v2.totalSales,
                                v1.count + v2.count
                        )
                );

        // -------- Elasticsearch Sink (new API) --------
        ElasticsearchSink<CategorySalesDTO> esSink = new Elasticsearch7SinkBuilder<CategorySalesDTO>()
                .setHosts(new HttpHost("localhost", 9200, "http"))
                .setEmitter((element, ctx, indexer) -> {
                    Map<String, Object> json = new HashMap<>();
                    json.put("category", element.getCategory());
                    json.put("totalSales", element.getTotalSales());
                    json.put("count", element.getCount());

                    indexer.add(Requests.indexRequest()
                            .id(element.getCategory())
                            .index("category-sales")
                            .source(json));
                })
                .setBulkFlushMaxActions(1)
                .build();

        categorySales.sinkTo(esSink);

        // -------- Elasticsearch Sink (raw joined order items) --------
        ElasticsearchSink<Tuple6<String, String, Float, Integer, Float, String>> rawSink =
                new Elasticsearch7SinkBuilder<Tuple6<String, String, Float, Integer, Float, String>>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setEmitter((element, ctx, indexer) -> {
                            Map<String, Object> json = new HashMap<>();
                            json.put("productId", element.f0);
                            json.put("productName", element.f1);
                            json.put("pricePerUnit", element.f2);
                            json.put("quantity", element.f3);
                            json.put("total", element.f4);
                            json.put("category", element.f5);

                            indexer.add(Requests.indexRequest()
                                    .index("order-items-raw")
                                    .source(json));
                        })
                        .setBulkFlushMaxActions(1)
                        .build();

        // sink raw joined data
        joined.sinkTo(rawSink);

        env.execute("Sales Analysis - Flink Streaming");
    }
}