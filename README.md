# Тестовое задание для Ecwid

https://github.com/Ecwid/new-job/blob/master/SQL-parser.md

Что бы настроить соединение с базой данных, нужно файл my_example.yml,
 переименовать в my.yml и заполнить его соответствующими данными, для подключения к базе.
 
# Структура проекта
- src
  - kotlin - исходники котлина
    - Db - пакет с классом для работы с базой данных
        - Connector - класс для работы с базой данных
        - DbInterface - интерфейс для Connector
  - java - исходники java
- test
    - kotlin
        - Connector - класс для тестирования методов класса Connector
    - java  
- my.yml - конфигурацыонный файл с параметрами, для подключения к базе
- .gitignore
- pom.xml    