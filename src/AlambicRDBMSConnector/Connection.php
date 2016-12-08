<?php

namespace AlambicRDBMSConnector;

class Connection
{

    private static $instances = [];
    private $connectionParams;
    private $connection;

    private function __construct($connectionParams)
    {
        $dbConfig = new \Doctrine\DBAL\Configuration();
        $this->connection = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $dbConfig);
    }

    public static function getInstance($connectionParams)
    {
        $key = serialize($connectionParams);
        if (!array_key_exists($key, self::$instances)) {
            self::$instances[$key] = new self($connectionParams);
        }
        return self::$instances[$key];
    }

    public function getConnection()
    {
        return $this->connection;
    }
}
