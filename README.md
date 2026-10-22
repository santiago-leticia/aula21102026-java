# Demonstração Spring Boot com Apache Kafka

Este projeto é um exemplo simples de como integrar o Spring Boot (Java 21) com o Apache Kafka para demonstrar um fluxo básico de Producer-Consumer de mensagens.

## Objetivos do Projeto

*   Configurar uma conexão com um broker Kafka.
*   Criar um tópico Kafka para troca de mensagens.
*   Implementar um **Producer** que envia mensagens para o tópico usando `KafkaTemplate`.
*   Implementar um **Consumer** que escuta mensagens de um tópico usando `@KafkaListener`.
*   Expor um endpoint REST para facilitar o envio de mensagens pelo Producer.

## Tecnologias Utilizadas

*   Java 21
*   Spring Boot 3.4.x (ou mais recente)
*   Spring for Apache Kafka
*   Gradle
*   Apache Kafka (executado via Docker)

## Pré-requisitos

*   JDK 21 ou superior instalado (recomendado: GraalVM Community Edition).
*   Gradle instalado.
*   Docker e Docker Compose instalados e rodando.
*   Uma IDE de sua preferência (**IntelliJ IDEA**, Eclipse, VS Code com extensões Java).

## Configuração e Execução

### 1. Iniciar o Kafka via Docker Compose

Antes de rodar a aplicação Spring Boot, você precisa ter uma instância do Kafka rodando. A forma mais fácil é usando Docker Compose:

```bash
cd docker
docker-compose up -d
```

Isso irá iniciar o Zookeeper e o Kafka. O Kafka estará disponível em `localhost:9092`.

Para parar os containers:
```bash
docker-compose down
```

### 2. Configuração da Aplicação

As configurações de conexão com o Kafka, bem como os nomes do tópico e grupo de consumidores, estão no arquivo `src/main/resources/application.properties`:

```properties
spring.application.name=kafka-demo

# Configurações do Servidor Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Configurações do Producer
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# Configurações do Consumer
spring.kafka.consumer.group-id=meu-grupo-consumidores
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.auto-offset-reset=earliest

# Nome do nosso tópico
app.kafka.topic.meu-topico=meu-primeiro-topico
```

Se o seu Kafka estiver rodando em um host ou porta diferente, ajuste essas propriedades.

### 3. Compilar e Rodar a Aplicação Spring Boot

Você pode rodar a aplicação de algumas formas:

**Pela IDE:**
Importe o projeto como um projeto Gradle e execute a classe principal `br.com.fiap.kafkademo.KafkaDemoApplication.java`.

**Via Gradle (no terminal, na raiz do projeto):**
```bash
./gradlew bootRun
```

Após iniciar, a aplicação se conectará ao Kafka. O tópico será criado automaticamente na primeira vez que uma mensagem for enviada (se `auto.create.topics.enable=true` estiver configurado no broker, que é o padrão).

## Testando a Aplicação

A aplicação expõe um endpoint REST para enviar mensagens para o tópico.

### Enviando uma Mensagem

Você pode usar ferramentas como `curl`, Postman ou qualquer cliente HTTP para enviar uma mensagem:

**Usando `curl`:**

```bash
curl -X POST http://localhost:8080/messages/send \
     -H "Content-Type: text/plain" \
     -d "Olá Kafka, esta é uma mensagem de teste!"
```

**Corpo da Requisição (Request Body):**
A mensagem que você quer enviar (texto puro).

**Resposta Esperada (Sucesso):**
*   Status: `200 OK`
*   Corpo: `Mensagem enviada para o tópico: Olá Kafka, esta é uma mensagem de teste!`

### Observando os Logs

Ao enviar a mensagem, observe o console da sua aplicação Spring Boot. Você deverá ver logs similares a estes:

1.  **Do Producer (`MessageProducer`):**
    ```
    INFO --- [nio-8080-exec-1] c.e.k.producer.MessageProducer         : Enviando mensagem: 'Olá Kafka, esta é uma mensagem de teste!' para o tópico 'meu-primeiro-topico'
    INFO --- [nio-8080-exec-1] c.e.k.producer.MessageProducer         : Mensagem enviada com sucesso!
    ```

2.  **Do Consumer (`MessageConsumer`):**
    ```
    INFO --- [ntainer#0-0-C-1] c.e.k.consumer.MessageConsumer         : Recebida mensagem do tópico 'meu-primeiro-topico': 'Olá Kafka, esta é uma mensagem de teste!'
    INFO --- [ntainer#0-0-C-1] c.e.k.consumer.MessageConsumer         : Mensagem processada com sucesso.
    ```

Isso confirma que a mensagem foi publicada pelo producer, passou pelo Kafka, e foi consumida e processada pelo consumer.

### Verificando com Ferramentas do Kafka

Você pode usar as ferramentas de linha de comando do Kafka para verificar tópicos e mensagens:

**Listar tópicos:**
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Descrever um tópico:**
```bash
docker exec -it kafka kafka-topics --describe --topic meu-primeiro-topico --bootstrap-server localhost:9092
```

**Consumir mensagens do tópico:**
```bash
docker exec -it kafka kafka-console-consumer --topic meu-primeiro-topico --from-beginning --bootstrap-server localhost:9092
```

## Estrutura do Projeto

*   `src/main/java/br/com/fiap/kafka-demo/`
    *   `KafkaDemoApplication.java`: Classe principal do Spring Boot.
    *   `configuration/KafkaConfig.java`: Configuração adicional do Kafka (se necessário).
    *   `controller/MessageController.java`: Controller REST para disparar o envio de mensagens.
    *   `producer/MessageProducer.java`: Serviço que envia mensagens para o Kafka.
    *   `consumer/MessageConsumer.java`: Componente que escuta e processa mensagens do tópico.
*   `src/main/resources/application.properties`: Arquivo de configuração da aplicação.
*   `docker/docker-compose.yml`: Arquivo Docker Compose para subir Kafka e Zookeeper.
*   `build.gradle`: Arquivo de definição do projeto e dependências.

## Próximos Passos (Sugestões)

*   Enviar objetos Java serializados (ex: para JSON usando Jackson).
*   Explorar particionamento de tópicos para escalabilidade.
*   Implementar múltiplos consumers no mesmo grupo para processamento paralelo.
*   Adicionar tratamento de erros e retry policies.
*   Configurar persistência e replicação de mensagens.
*   Explorar Kafka Streams para processamento de stream em tempo real.
*   Implementar compactação de tópicos para log-based storage.
```