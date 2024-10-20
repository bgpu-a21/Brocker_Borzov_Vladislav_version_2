package org.broker;

import java.io.*; // Импортируем классы для работы с вводом и выводом
import java.net.ServerSocket; // Импортируем класс для работы с серверными сокетами
import java.net.Socket; // Импортируем класс для работы с клиентскими сокетами
import java.util.*; // Импортируем классы из стандартной библиотеки для работы с коллекциями
import java.nio.charset.StandardCharsets; // Импортируем стандартные кодировки символов
import java.nio.ByteBuffer; // Импортируем класс для работы с байтовыми буферами

public class Server {

    public static final int PORT = 1234; // Определяем константу для порта, на котором будет работать сервер

    // Коллекция для хранения именованных очередей сообщений
    static Map<String, Queue<String>> namedQueues = new HashMap<>();
    // Коллекция для хранения таймеров, связанных с очередями
    static Map<String, Timer> queueTimers = new HashMap<>();
    // Переменная для хранения очереди, из которой извлекаются сообщения
    static Queue<String> retrievedQueue;

    public static void main(String[] args) { // Главный метод, точка входа в программу
        try (ServerSocket serverSocket = new ServerSocket(PORT)) { // Создаем серверный сокет для прослушивания порта
            System.out.println("Сервер запущен."); // Сообщаем о запуске сервера

            while (true) { // Бесконечный цикл для обработки входящих соединений
                new ClientHandler(serverSocket.accept()).start(); // Принимаем входящее соединение и обрабатываем его в новом потоке
            }
        } catch (IOException e) { // Обработка исключений ввода-вывода
            System.err.println("Ошибка запуска сервера: " + e.getMessage()); // Сообщаем об ошибке запуска сервера
        }
    }

    // Метод для запуска таймера очереди
    public static void startQueueTimer(String queueName) {
        Timer timer = new Timer(true); // Создаем таймер с флагом, что он будет работать в фоновом режиме
        timer.scheduleAtFixedRate(new TimerTask() { // Планируем задачу, которая будет выполняться периодически
            @Override
            public void run() { // Метод, который будет выполнен при срабатывании таймера
                Queue<String> queue = namedQueues.get(queueName); // Получаем очередь по имени
                if (queue != null && queue.isEmpty()) { // Если очередь существует и пуста
                    namedQueues.remove(queueName); // Удаляем очередь из коллекции
                    queueTimers.remove(queueName); // Удаляем таймер, связанный с очередью
                    System.out.println("Очередь '" + queueName + "' удалена из-за пустоты."); // Сообщаем об удалении очереди
                    timer.cancel(); // Останавливаем таймер
                }
            }
        }, 30 * 1000, 30 * 1000); // Запускаем задачу с интервалом 30 секунд

        queueTimers.put(queueName, timer); // Сохраняем таймер в коллекции
    }

    // Вложенный класс для обработки клиентских соединений
    public static class ClientHandler extends Thread {

        public Socket socket; // Сокет для связи с клиентом
        public InputStream inputStream; // Поток для чтения данных от клиента
        public OutputStream outputStream; // Поток для отправки данных клиенту
        public String currentQueue = null; // Текущая очередь клиента
        private int expectedBytes = 0; // Ожидаемое количество байтов для сообщения
        public String queueNameMes = null; // Имя очереди для текущего соединения

        // Конструктор, принимающий сокет клиента
        public ClientHandler(Socket socket) {
            this.socket = socket; // Инициализация сокета
        }

        @Override
        public void run() { // Метод, выполняемый при запуске потока
            try {
                inputStream = socket.getInputStream(); // Получаем поток ввода для чтения данных от клиента
                outputStream = socket.getOutputStream(); // Получаем поток вывода для отправки данных клиенту

                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)); // Создаем BufferedReader для удобного чтения строк

                String clientCommand; // Переменная для хранения команды клиента
                while ((clientCommand = reader.readLine()) != null) { // Читаем команды клиента
                    String[] parts = clientCommand.split(" ", 2); // Разделяем команду на части
                    String action = parts[0]; // Получаем действие из первой части команды

                    // Обработка команд в зависимости от действия
                    switch (action) {
                        case "send": // Если команда "send"
                            handleSend(parts); // Обрабатываем отправку сообщения
                            break;
                        case "receive": // Если команда "receive"
                            handleReceive(parts); // Обрабатываем получение сообщения
                            queueNameMes = null; // Сбрасываем имя очереди
                            break;
                        case "message": // Если команда "message"
                            if(queueNameMes != null) { // Если имя очереди задано
                                if (namedQueues.containsKey(queueNameMes)) { // Если очередь существует
                                    handleMessage(parts); // Обрабатываем сообщение
                                } else {
                                    sendResponse("Очередь была удалена, введите имя другой очереди."); // Сообщаем об ошибке
                                }
                            } else { // Если имя очереди не задано
                                sendResponse("Прежде чем вводить сообщение необходимо создать очередь или задать имя очереди."); // Сообщаем об ошибке
                            }
                            break;
                        default: // Если команда не распознана
                            sendResponse("Неизвестная команда."); // Сообщаем об ошибке
                    }
                }

            } catch (IOException e) { // Обработка исключений ввода-вывода
                System.out.println("Клиент отключился."); // Сообщаем о том, что клиент отключился
            } finally { // Блок finally для выполнения кода после завершения блока try
                try {
                    socket.close(); // Закрываем сокет
                } catch (IOException e) { // Обработка ошибок при закрытии соединения
                    System.err.println("Ошибка при закрытии соединения: " + e.getMessage()); // Сообщаем об ошибке
                }
            }
        }

        // Метод для отправки ответа клиенту
        private void sendResponse(String response) {
            try {
                byte[] messageBytes = (response + "\n").getBytes(StandardCharsets.UTF_8); // Преобразуем строку в массив байтов
                int messageLength = messageBytes.length; // Получаем длину сообщения
                byte[] lengthBytes = ByteBuffer.allocate(4).putInt(messageLength).array(); // Преобразуем длину сообщения в 4 байта
                outputStream.write(lengthBytes); // Отправляем длину сообщения клиенту
                outputStream.flush(); // Очищаем буфер вывода

                outputStream.write(messageBytes); // Отправляем само сообщение клиенту
                outputStream.flush(); // Очищаем буфер вывода
            } catch (IOException e) { // Обработка ошибок при отправке
                System.err.println("Ошибка отправки ответа клиенту: " + e.getMessage()); // Сообщаем об ошибке
            }
        }

        // Метод для обработки команды send
        private void handleSend(String[] parts) {
            if (parts.length < 2) { // Если аргументов меньше двух
                sendResponse("Неверный формат команды send."); // Сообщаем об ошибке
                return; // Выходим из метода
            }

            queueNameMes = parts[1]; // Получаем имя очереди
            if (!namedQueues.containsKey(queueNameMes)) { // Если очередь не существует
                namedQueues.put(queueNameMes, new LinkedList<>()); // Создаем новую очередь и добавляем ее в коллекцию
                startQueueTimer(queueNameMes); // Запускаем таймер для этой очереди
                sendResponse("Очередь '" + queueNameMes + "' создана."); // Сообщаем о создании очереди
                currentQueue = queueNameMes; // Устанавливаем текущую очередь
            } else {
                sendResponse("Очередь '" + queueNameMes + "' уже существует."); // Сообщаем о том, что очередь уже существует
            }
        }

        // Метод для обработки команды receive
        private void handleReceive(String[] parts) {
            if (parts.length < 2) { // Если аргументов меньше двух
                sendResponse("Неверный формат команды receive."); // Сообщаем об ошибке
                return; // Выходим из метода
            }

            String queueName = parts[1]; // Получаем имя очереди
            Queue<String> queue = namedQueues.get(queueName); // Получаем очередь по имени

            if (queue == null) { // Если очередь не существует
                sendResponse("Очередь '" + queueName + "' не существует."); // Сообщаем об ошибке
                currentQueue = null; // Сбрасываем текущую очередь
            } else if (queue.isEmpty()) { // Если очередь пуста
                sendResponse("Очередь '" + queueName + "' пуста."); // Сообщаем об ошибке
                currentQueue = null; // Сбрасываем текущую очередь
            } else { // Если очередь существует и не пуста
                sendResponse("Сообщение из очереди: " + queue.poll()); // Извлекаем сообщение из очереди и отправляем клиенту
                currentQueue = null; // Сбрасываем текущую очередь
            }
        }

        // Метод для обработки команды message
        public void handleMessage(String[] parts) {
            if (parts.length < 2) { // Если аргументов меньше двух
                sendResponse("Неправильный формат команды message."); // Сообщаем об ошибке
                return; // Выходим из метода
            }

            try {
                expectedBytes = Integer.parseInt(parts[1]); // Получаем ожидаемое количество байтов
                readBytes(); // Начинаем чтение байтов
            } catch (NumberFormatException e) { // Если не удалось преобразовать строку в число
                sendResponse("Ошибка: неверный формат длины сообщения."); // Сообщаем об ошибке
            }
        }

        // Метод для чтения ожидаемого количества байтов
        private void readBytes() {
            try {
                byte[] buffer = new byte[expectedBytes]; // Создаем буфер для хранения байтов
                int totalBytesRead = 0; // Счетчик прочитанных байтов
                while (totalBytesRead < expectedBytes) { // Пока не прочитано ожидаемое количество байтов
                    buffer[totalBytesRead] = (byte) inputStream.read(); // Читаем байт из входного потока
                    totalBytesRead++; // Увеличиваем счетчик
                }

                String str = new String(buffer, StandardCharsets.UTF_8); // Преобразуем прочитанные байты в строку

                if (namedQueues.containsKey(queueNameMes)) { // Если очередь существует
                    retrievedQueue = namedQueues.get(queueNameMes); // Получаем очередь
                    if (retrievedQueue != null) { // Если очередь не равна null
                        retrievedQueue.add(str); // Добавляем сообщение в очередь
                        sendResponse("Сообщение '" + str + "' добавлено в очередь '" + queueNameMes + "'."); // Сообщаем об успешном добавлении
                    } else {
                        sendResponse("Ошибка: очередь не выбрана."); // Сообщаем об ошибке
                    }

                } else { // Если очередь была удалена
                    sendResponse("Очередь с именем '" + queueNameMes + "' была удалена."); // Сообщаем об ошибке
                }

            } catch (IOException e) { // Обработка ошибок чтения
                System.err.println("Ошибка чтения байтовых данных: " + e.getMessage()); // Сообщаем об ошибке
            }
        }
    }
}
