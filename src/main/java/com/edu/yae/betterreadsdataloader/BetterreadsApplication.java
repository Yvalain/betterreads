package com.edu.yae.betterreadsdataloader;

import com.edu.yae.betterreadsdataloader.author.Author;
import com.edu.yae.betterreadsdataloader.author.AuthorRepository;
import com.edu.yae.betterreadsdataloader.book.Book;
import com.edu.yae.betterreadsdataloader.book.BookRepository;
import com.edu.yae.betterreadsdataloader.connection.DataStaxAstraProperties;
import jakarta.annotation.PostConstruct;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsApplication {
    @Autowired
    private AuthorRepository authorRepository;
    @Autowired
    private BookRepository bookRepository;
    @Value("${datadump.location.author}")
    private String authorDumpLocation;
    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsApplication.class, args);
    }

    private void initAuthors() {

        Path path = Paths.get(authorDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    System.out.println("Saving author " + author.getName() + " ----");
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks() {
        Path path = Paths.get(worksDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Book book = new Book();
                    book.setId(jsonObject.getString("key").replace("/works", ""));
                    book.setName(jsonObject.optString("title"));

                    JSONObject descriptionObj = jsonObject.optJSONObject("description");
                    if (descriptionObj != null) {
                        book.setDescription(descriptionObj.optString("value"));
                    }

                    JSONArray coversJsonArr = jsonObject.optJSONArray("covers");
                    if (coversJsonArr != null) {
                        List<String> coverIds = new ArrayList<>();
                        for (int i = 0; i < coversJsonArr.length(); i++) {
                            coverIds.add(String.valueOf(coversJsonArr.getInt(i)));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsJsonArr = jsonObject.optJSONArray("authors");
                    if (authorsJsonArr != null) {
                        List<String> authorIds = new ArrayList<>();
                        for (int i = 0; i < authorsJsonArr.length(); i++) {
                            authorIds.add(authorsJsonArr.getJSONObject(i).getJSONObject("author")
                                    .getString("key").replace("/authors", ""));
                        }
                        book.setAuthorId(authorIds);
                        List<String> authorNames = authorIds.stream().map(id ->
                                authorRepository.findById(id)).map(optionalAuthor -> {
                            if (optionalAuthor.isEmpty()) return "Unknown Author";
                            return optionalAuthor.get().getName();
                        }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);
                    }

                    JSONObject publishedObj = jsonObject.optJSONObject("created");
                    if (publishedObj != null) {
                        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                        String dateStr = publishedObj.getString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, dateFormatter));
                    }

                    System.out.println("Saving book " + book.getName() + " ----");
                    bookRepository.save(book);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void start() {
        initAuthors();
        initWorks();
    }


    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
