package org.broker; // Определение пакета, в котором находится класс Client

import java.io.*; // Импортируем классы для работы с вводом и выводом
import java.net.Socket; // Импортируем класс для работы с сокетами
import java.net.UnknownHostException; // Импортируем класс для обработки ошибок, связанных с незнакомыми хостами
import java.nio.charset.StandardCharsets; // Импортируем стандартные кодировки символов
import java.util.Scanner; // Импортируем класс Scanner для считывания пользовательского ввода
import java.nio.ByteBuffer; // Импортируем класс для работы с байтовыми буферами

public class Client {

    public static final int PORT = 1234; // Определяем константу для порта, на котором будет работать сервер

    private InputStream inputStream; // Поток для чтения данных с сокета
    private OutputStream outputStream; // Поток для записи данных в сокет
    private Scanner scanner; // Объект Scanner для считывания пользовательского ввода
    private String queueNameMes; // Переменная для хранения имени текущей очереди сообщений

    public static void main(String[] args) { // Главный метод, точка входа в программу
        new Client().startClient(); // Создаем экземпляр Client и запускаем метод startClient
    }

    public void startClient() { // Метод для запуска клиента
        try (Socket socket = new Socket("localhost", PORT)) { // Устанавливаем соединение с сервером на localhost и указанном порту
            scanner = new Scanner(System.in); // Инициализируем Scanner для считывания ввода с клавиатуры
            inputStream = socket.getInputStream(); // Получаем поток ввода для чтения данных от сервера
            outputStream = socket.getOutputStream(); // Получаем поток вывода для отправки данных на сервер

            System.out.print("Введите команду: "); // Запрашиваем у пользователя ввод команды

            new Thread(() -> { // Создаем новый поток для обработки входящих сообщений от сервера
                try {
                    while (true) { // Бесконечный цикл для чтения сообщений
                        byte[] lengthBuffer = new byte[4]; // Буфер для чтения длины сообщения (4 байта)
                        int lengthBytesRead = inputStream.read(lengthBuffer); // Читаем 4 байта, которые содержат длину сообщения

                        if (lengthBytesRead == -1) { // Если сервер закрыл соединение
                            System.out.println("Соединение потеряно."); // Сообщаем пользователю о потере соединения
                            break; // Выходим из цикла
                        }

                        if (lengthBytesRead != 4) { // Если не удалось прочитать 4 байта
                            System.err.println("Не удалось прочитать длину сообщения."); // Сообщаем об ошибке
                            continue; // Продолжаем цикл
                        }

                        int messageLength = bytesToInt(lengthBuffer); // Преобразуем 4 байта в длину сообщения

                        byte[] messageBuffer = new byte[messageLength]; // Создаем буфер для чтения самого сообщения
                        int bytesRead = inputStream.read(messageBuffer); // Читаем сообщение из входного потока

                        if (bytesRead == messageLength) { // Если количество прочитанных байтов соответствует ожидаемому
                            String message = new String(messageBuffer, StandardCharsets.UTF_8); // Преобразуем байты в строку
                            System.out.println("Сервер: " + message); // Выводим сообщение от сервера
                        } else {
                            System.err.println("Ожидалось " + messageLength + " байтов, но получено " + bytesRead); // Сообщаем о несоответствии длины сообщения
                        }

                        System.out.print("Введите команду: "); // Запрашиваем ввод следующей команды
                    }
                } catch (IOException e) { // Обработка ошибок ввода-вывода
                    System.out.println("Соединение потеряно."); // Сообщаем пользователю о потере соединения
                }
            }).start(); // Запускаем поток для обработки сообщений

            while (true) { // Бесконечный цикл для ввода команд
                String command = scanner.nextLine().trim(); // Считываем введенную команду

                if (command.isEmpty()) { // Если команда пустая
                    System.out.println("Команда не должна быть пустой."); // Сообщаем об ошибке
                    continue; // Продолжаем цикл
                }

                if (command.equalsIgnoreCase("exit")) { // Если команда "exit"
                    System.out.println("Завершение работы клиента."); // Сообщаем о завершении работы
                    break; // Выходим из цикла
                }

                if (command.equalsIgnoreCase("help")) { // Если команда "help"
                    printHelp(); // Выводим справку по командам
                    System.out.print("Введите команду: "); // Запрашиваем ввод следующей команды
                    continue; // Продолжаем цикл
                }

                if (command.startsWith("message")) { // Если команда начинается с "message"
                    String[] len = command.split(" ", 2); // Разделяем команду на части, чтобы получить длину сообщения
                    sendMessage(len[1]); // Отправляем сообщение
                } else {
                    if (command.startsWith("send")) { // Если команда начинается с "send"
                        String[] lenSend = command.split(" ", 2); // Разделяем команду на части, чтобы получить имя очереди
                        queueNameMes = lenSend[1]; // Сохраняем имя очереди
                    } else {
                        if (command.startsWith("receive")) { // Если команда начинается с "receive"
                            queueNameMes = null; // Сбрасываем имя очереди
                        }
                    }
                    outputStream.write((command + "\n").getBytes(StandardCharsets.UTF_8)); // Отправляем команду на сервер
                    outputStream.flush(); // Очищаем буфер вывода
                }
            }

            /*send myQueue1
              message 13
              Hello, world1
              receive myQueue1*/

            /*send myQueue1
              message 12
              Hello world2
              receive myQueue1
            */

            /*send jokesQueue
              message 41
              Why don’t scientists trust atoms? They...
              receive jokesQueue
            */

            /*send myQueue4
              message 10
              OpenAI bot
              receive myQueue4
            */

            /*send myQueue2
              message 11
              Hi there!!!
              receive myQueue2
           */

        } catch (UnknownHostException e) { // Обработка исключения, если хост неизвестен
            System.err.println("Не удалось подключиться к серверу: " + e.getMessage()); // Сообщаем об ошибке подключения
        } catch (IOException e) { // Обработка исключения ввода-вывода
            System.err.println("Ошибка ввода-вывода: " + e.getMessage()); // Сообщаем об ошибке ввода-вывода
        } finally { // Блок finally для выполнения кода после завершения блока try
            if (scanner != null) { // Если scanner не равен null
                scanner.close(); // Закрываем Scanner
            }
        }
    }

    private int bytesToInt(byte[] bytes) { // Метод для преобразования массива байтов в целое число
        return ByteBuffer.wrap(bytes).getInt(); // Используем ByteBuffer для преобразования
    }

    private void printHelp() { // Метод для вывода справки
        System.out.println("Доступные команды:"); // Сообщаем о доступных командах
        System.out.println("send <queue> - подключиться как отправитель к очереди"); // Описание команды send
        System.out.println("receive <queue> - подключиться как получатель к очереди"); // Описание команды receive
        System.out.println("message <length>" + "\\" + "n" + "<message> - отправить сообщение в текущую очередь (длина сообщения - это число символов)"); // Описание команды message
        System.out.println("exit - выход из клиента"); // Описание команды exit
    }

    private void sendMessage(String messageLength) { // Метод для отправки сообщения
        try {
            int number = Integer.parseInt(messageLength); // Преобразуем строку с длиной сообщения в целое число
            outputStream.write(("message " + number + "\n").getBytes(StandardCharsets.UTF_8)); // Отправляем команду message с длиной сообщения
            outputStream.flush(); // Очищаем буфер вывода
            if (queueNameMes != null) { // Если имя очереди не равно null
                boolean messang = true; // Флаг для управления циклом
                do {
                    String secondLine = scanner.nextLine().trim(); // Считываем сообщение от пользователя
                    if (secondLine.length() != number) { // Если длина введенного сообщения не соответствует ожидаемой
                        System.out.println("Длина сообщения не соответствует указанной, введите сообщение ещё раз:"); // Сообщаем об ошибке
                    } else { // Если длина сообщения соответствует
                        byte[] messageBytes = secondLine.getBytes(StandardCharsets.UTF_8); // Преобразуем сообщение в массив байтов
                        for (int i = 0; i < messageBytes.length; i++) { // Отправляем байты сообщения по одному
                            outputStream.write(messageBytes[i]); // Отправляем байт
                            outputStream.flush(); // Очищаем буфер вывода
                        }
                        messang = false; // Устанавливаем флаг в false для выхода из цикла
                    }
                } while (messang); // Продолжаем цикл, пока флаг messang равен true
            }

        } catch (IOException e) { // Обработка исключений ввода-вывода
            System.err.println("Ошибка при отправке сообщения: " + e.getMessage()); // Сообщаем об ошибке отправки сообщения
        }
    }
}
