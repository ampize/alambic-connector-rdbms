<?php

namespace AlambicRDBMSConnector;

use \Exception;

class Connector
{
    public function __invoke($payload=[])
    {
        if (isset($payload['response'])) {
            return $payload;
        }

        $configs=isset($payload["configs"]) ? $payload["configs"] : [];

        $baseConfig=isset($payload["connectorBaseConfig"]) ? $payload["connectorBaseConfig"] : [];

        if (empty($baseConfig["databaseUrl"])) {
            throw new Exception('Database url is required');
        }

        if (empty($configs["table"])) {
            throw new Exception('Table is required');
        }

        $config = new \Doctrine\DBAL\Configuration();

        $connectionParams = array(
            'url' => $baseConfig["databaseUrl"]
        );
        $client = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $config);

        return $payload["isMutation"] ? $this->execute($payload, $client) : $this->resolve($payload, $client, $configs);
    }

    public function resolve($payload=[],$client,$configs){

        $args=isset($payload["args"]) ? $payload["args"] : [];

        $multivalued=isset($payload["multivalued"]) ? $payload["multivalued"] : false;

        $queryBuilder = $client->createQueryBuilder();

        $queryBuilder->from($configs["table"]);

        $fields = [];
        if (!empty($payload['pipelineParams']['argsDefinition'])) {
            // only query scalar types
            foreach($payload['pipelineParams']['argsDefinition'] as $key => $value) {
                if (in_array($value['type'], ['Int', 'Float', 'Boolean', 'String', 'ID'])) {
                    $fields[] = $key;
                } else {
                    $fields[] = reset($value['relation']);
                }
            }
        }
        if (empty($fields)) {
            $fieldList = '*';
        } else {
            $fieldList = implode(',',$fields);
        }

        $queryBuilder->select($fieldList);

        foreach ($payload['args'] as $key => $value) {
            $type = isset($payload['pipelineParams']['argsDefinition'][$key]['type']) ? $payload['pipelineParams']['argsDefinition'][$key]['type'] : 'unknown';
            switch ($type) {
                case 'Int':
                case 'Float':
                case 'Boolean':
                    $queryBuilder->andWhere("$key=$value");
                    break;
                case 'String':
                case 'ID':
                case 'unknown':
                    $queryBuilder->andWhere("$key=\"$value\"");
                    break;
            }
        }

        if (!empty($payload['pipelineParams']['start'])) $queryBuilder->setFirstResult($payload['pipelineParams']['start']);

        if (!empty($payload['pipelineParams']['limit'])) $queryBuilder->setMaxResults($payload['pipelineParams']['limit']);

        if (!empty($payload['pipelineParams']['orderBy'])) {
            $orderByDirection = !empty($payload['pipelineParams']['orderByDirection']) ? $payload['pipelineParams']['orderByDirection'] : 'DESC';
            $queryBuilder->orderBy($payload['pipelineParams']['orderBy'], $orderByDirection);
        }

        $sql = $queryBuilder->getSQL();
        $results = $client->query($sql)->fetchAll();

        if ($multivalued) {
            $payload["response"] = (!empty($results)) ? $results : null;
        } else {
            $payload["response"] = (!empty($results)) ? $results[0] : null;
        }

        return $payload;
    }

    public function execute($payload=[],$diffbot){
        throw new Exception('WIP');
    }

}
