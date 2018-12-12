<?php
/**
 * Created by PhpStorm.
 * User: ema
 * Date: 31.01.2018
 * Time: 11:06
 * Version: v1.0
 */

namespace StompMQ;

use Stomp\Client;
use Stomp\Exception\StompException;
use Stomp\Network\Connection;
use Stomp\SimpleStomp;
use Stomp\Transport\Bytes;
use Stomp\Transport\Frame;

/**
 * Class StompMQ реализующий взаимодействие с MQ по протоколу Stomp
 * Один экземпляр реализует подключение только к одной очереди
 * @package app\components
 */
trait TraitStompMQ
{
    /**
     * Последнее полученое сообщение
     * @var Frame
     */
    public $frame;
    /**
     * Имя сервера "tcp://localhost:61613"
     * @var string
     */
    protected $host;
    /**
     * Имя очереди
     * @var string
     */
    protected $queue;
    /**
     * @var SimpleStomp
     */
    protected $stomp;
    /**
     * @var Connection
     */
    protected $connection;
    /**
     * @var Client
     */
    protected $client;
    /**
     * @var callable
     */
    protected $outer_sig_handler;
    /**
     * @var bool Отмечает подписан ли на очередь
     */
    protected $signed = false;

    /**
     * Проверка наличия соединения с MQ и если его нет то установка его
     * @return bool
     * @throws \Stomp\Exception\ConnectionException
     */
    public function open(): bool
    {
        if ($this->stomp) {
            return true;
        }
        $this->connection = new Connection($this->host);
        $this->connection->setReadTimeout(360000, 0);
        $this->client = new Client($this->connection);
        $this->stomp = new SimpleStomp($this->client);
        return true;
    }
    /**
     * Returns the value of an object property.
     *
     * Do not call this method directly as it is a PHP magic method that
     * will be implicitly called when executing `$value = $object->property;`.
     * @param string $name the property name
     * @return mixed the property value
     * @see __set()
     * @throws \Exception
     */
    public function __get($name)
    {
        $getter = 'get' . $name;
        if (method_exists($this, $getter)) {
            return $this->$getter();
        } elseif (method_exists($this, 'set' . $name)) {
            throw new \Exception('Getting write-only property: ' . get_class($this) . '::' . $name);
        }

        throw new \Exception('Getting unknown property: ' . get_class($this) . '::' . $name);
    }

    /**
     * Sets value of an object property.
     *
     * Do not call this method directly as it is a PHP magic method that
     * will be implicitly called when executing `$object->property = $value;`.
     * @param string $name the property name or the event name
     * @param mixed $value the property value
     * @throws \Exception
     * @see __get()
     */
    public function __set($name, $value)
    {
        $setter = 'set' . $name;
        if (method_exists($this, $setter)) {
            $this->$setter($value);
        } elseif (method_exists($this, 'get' . $name)) {
            throw new \Exception('Setting read-only property: ' . get_class($this) . '::' . $name);
        } else {
            throw new \Exception('Setting unknown property: ' . get_class($this) . '::' . $name);
        }
    }

    public function getQueue()
    {
        return $this->queue;
    }


    /**
     * @param $var
     * @throws \Exception
     */
    public function setQueue($var)
    {
        if ($this->queue === null) {
            $this->queue = $var;
        } else {
            throw new StompException('Нельзя перезадать очередь');
        }
    }

    public function getHost()
    {
        return $this->host;
    }

    /**
     * @param $var
     * @throws \Exception
     */
    public function setHost($var)
    {
        if ($this->host === null) {
            $this->host = $var;
        } else {
            throw new StompException('Нельзя перезадать хост');
        }
    }

    /**
     * Закрывает соединения с MQ
     */
    public function close()
    {
        $this->unsubscribe();
        if (isset($this->client)) {
            $this->client->disconnect();
        }
        if (isset($this->connection)) {
            $this->connection->disconnect();
        }
        return true;
    }

    /**
     * Подписывание на очередь
     */
    protected function subscribe()
    {
        if ($this->signed) {
            return true;
        }
        $this->open();
        $this->stomp->subscribe($this->queue, $this->queue, 'client-individual');
        $this->signed = true;
        return true;
    }

    /**
     * Отписывание от очереди
     */
    public function unsubscribe()
    {
        if ($this->signed === false) {
            return true;
        }
        try {
            $this->stomp->unsubscribe($this->queue, $this->queue);
        } catch (StompException $e) {
            $this->signed = false;
            return true;
        }
        $this->signed = false;
        return true;
    }

    /**
     * Отправка сообщение в MQ
     * @param string $msg
     */
    public function write($msg)
    {
        $this->open();
        return $this->stomp->send($this->queue, new Bytes($msg));
    }

    /**
     * Читение 1 записи из MQ
     * @return Frame
     * @throws \Stomp\Exception\ConnectionException
     */
    public function read(): Frame
    {
        $this->open();
        $this->subscribe();
        $this->frame = $this->stomp->read();
        return $this->frame;
    }

    /**
     * Отметка запроса как обработаного
     * @param Frame $frame
     */
    public function ack(Frame $frame = null)
    {
        $frame = $frame ?? $this->frame;
        $this->stomp->ack($frame);
        return true;
    }

    /**
     * Отметка запроса как НЕ обработаного
     * @param Frame $frame
     */
    public function nack(Frame $frame = null)
    {
        $frame = $frame ?? $this->frame;
        if (!$frame) {
            return false;
        }
        $this->stomp->nack($frame);
        return true;
    }

    /**
     * Наблюдение за очередью, и при появлении сообщение обрабатывает его.
     * @param callable $callback
     * @throws \Exception
     */
    public function watch($callback)
    {
        $this->outer_sig_handler = \pcntl_signal_get_handler(\SIGTERM);
        \pcntl_signal(\SIGTERM, [$this, 'sig_die']); //Включаем обработчик сигнала kill
        declare(ticks=1) { //Проверяем сигнал для kill только в безопасной области, пока апись не получена из очереди
            $record = $this->read();
        }
        \pcntl_signal(\SIGTERM, $this->outer_sig_handler);

        try {
            \call_user_func($callback, $record->body);
        } catch (\Exception $e) {
            $this->nack($record);
            throw $e;
        }
        $this->ack($record);
        return true;
    }

    /**
     * Обработчик сигнала kill
     */
    public function sig_die()
    {
        $this->close();
        $outer_sig_handler = $this->outer_sig_handler;
        if (\is_callable($outer_sig_handler)) {
            $outer_sig_handler();
        }
    }
}
