<?php

namespace AlambicRDBMSConnector;

use Alambic\Exception\ConnectorInternal;
use Alambic\Exception\ConnectorArgs;
use Alambic\Exception\ConnectorConfig;
use Alambic\Exception\ConnectorUsage;
use \Exception;


class Connector extends \Alambic\Connector\AbstractConnector
{
    protected $client;
    protected $idField="id";
    protected $requiredConfig = [
        'db' => 'Database name is required',
        'user' => 'Database user is required',
        'password' => 'Password is required',
        'host' => 'Host is required',
        'driver' => 'Driver is required',
        'port' => 'Port is required',
        'table' => 'Table name is required',
    ];
    protected $limit = 15;
    protected $orderByDirection = 'DESC';
    protected $implementedOperators=[
        "eq"=>"=",
        "ne"=>"<>",
        "lt"=>"<",
        "lte"=>"<=",
        "gt"=>">",
        "gte"=>">=",
        "like"=>"LIKE",
        "notLike"=>"NOT LIKE",
        "between"=>"BETWEEN",
        "notBetween"=>"NOT BETWEEN",
        "in"=>"IN",
        "notIn"=>"NOT IN",
    ];

    public function __invoke($payload = [])
    {
        if (isset($payload['response'])) {
            return $payload;
        }

        $this->setPayload($payload);
        $this->checkConfig();
        $this->idField=!empty($this->config["idField"]) ? $this->idField=$this->config["idField"] : "id";
        $connectionParams = array(
            'dbname' => $this->config['db'],
            'user' => $this->config['user'],
            'password' => $this->config['password'],
            'host' => $this->config['host'],
            'driver' => $this->config['driver'],
            'port' => $this->config['port'],
        );
        $this->client = Connection::getInstance($connectionParams)->getConnection();

        return $payload['isMutation'] ? $this->execute($payload) : $this->resolve($payload);
    }

    private function resolve($payload)
    {
        $queryBuilder = $this->client->createQueryBuilder();

        $queryBuilder->from($this->config['table']);

        $selectString='*';
        if(!empty($this->groupBy)){
            $selectString=implode(',', $this->groupBy);
        }
        $queryBuilder->select($selectString);

        foreach ($this->args as $key => $value) {
            $type = isset($this->argsDefinition[$key]['type']) ? $this->argsDefinition[$key]['type'] : 'unknown';
            switch ($type) {
                case 'Int':
                case 'Float':
                case 'Boolean':
                    $queryBuilder->andWhere("$key=$value");
                    break;
                case 'Date':
                    $filterDate = new \DateTime($value);
                    $queryBuilder->andWhere("$key=:filterDate");
                    $queryBuilder->setParameter('filterDate', $filterDate);
                    break;
                case 'String':
                case 'ID':
                case 'unknown':
                    $queryBuilder->andWhere("$key=\"$value\"");
                    break;
            }
        }

        if ($this->multivalued) {
            if($this->filters) {
                if (!empty($this->filters["scalarFilters"])) {

                    foreach ($this->filters["scalarFilters"] as $scalarFilter) {
                        if (isset($this->implementedOperators[$scalarFilter["operator"]])) {
                            $expression=$scalarFilter["field"].' '.$this->implementedOperators[$scalarFilter["operator"]].' '.$this->getValueForType($scalarFilter["field"],$scalarFilter["value"],$scalarFilter["operator"]);
                            if($scalarFilter["value"]===null&&($scalarFilter["operator"]=='ne'||$scalarFilter["operator"]=='eq')) {
                                $expression= $scalarFilter["operator"]=='ne' ? $scalarFilter["field"].' IS NOT NULL' : $scalarFilter["field"].' IS NULL';
                            }
                            if(isset($this->filters["operator"])&&$this->filters["operator"]=="or"){
                                $queryBuilder->orWhere($expression);
                            } else {
                                $queryBuilder->andWhere($expression);
                            }
                        }
                    }
                }
                if(!empty($this->filters["betweenFilters"])){
                    foreach($this->filters["betweenFilters"] as $betweenFilter){
                        if($betweenFilter["operator"]=='between'){
                            if(isset($this->filters["operator"])&&$this->filters["operator"]=="or"){
                                $queryBuilder->orWhere($betweenFilter["field"].' '.$this->implementedOperators[$betweenFilter["operator"]].' '.$this->getValueForType($betweenFilter["field"],$betweenFilter["min"],$betweenFilter["operator"]).' AND '.$this->getValueForType($betweenFilter["field"],$betweenFilter["max"],$betweenFilter["operator"]));
                            } else {
                                $queryBuilder->andWhere($betweenFilter["field"].' '.$this->implementedOperators[$betweenFilter["operator"]].' '.$this->getValueForType($betweenFilter["field"],$betweenFilter["min"],$betweenFilter["operator"]).' AND '.$this->getValueForType($betweenFilter["field"],$betweenFilter["max"],$betweenFilter["operator"]));
                            }
                        }
                    }
                }
                if (!empty($this->filters["arrayFilters"])) {
                    foreach ($this->filters["arrayFilters"] as $arrayFilter) {
                        if (isset($this->implementedOperators[$arrayFilter["operator"]])&&!empty($arrayFilter["value"])) {
                            $refinedValue='(';
                            $hasFirst=false;
                            foreach($arrayFilter["value"] as $value){
                                if($hasFirst){
                                    $refinedValue=$refinedValue.', ';
                                } else {
                                    $hasFirst=true;
                                }
                                $refinedValue=$refinedValue.$this->getValueForType($arrayFilter["field"],$value,$arrayFilter["operator"]);
                            }
                            $refinedValue=$refinedValue.')';
                            if(isset($this->filters["operator"])&&$this->filters["operator"]=="or"){
                                $queryBuilder->orWhere($arrayFilter["field"].' '.$this->implementedOperators[$arrayFilter["operator"]].' '.$refinedValue);
                            } else {
                                $queryBuilder->andWhere($arrayFilter["field"].' '.$this->implementedOperators[$arrayFilter["operator"]].' '.$refinedValue);
                            }
                        }
                    }
                }
            }
            $queryBuilder->setFirstResult($this->start);
            $queryBuilder->setMaxResults($this->limit);
            if (!empty($this->orderBy)){
                $queryBuilder->orderBy($this->orderBy, $this->orderByDirection);
            }
            if(!empty($this->groupBy)){
                $queryBuilder->groupBy($this->groupBy);
            }
        }

        $sql = $queryBuilder->getSQL();
        $results = $this->client->query($sql)->fetchAll();

        if ($this->multivalued) {
            $payload['response'] = (!empty($results)) ? $results : null;
        } else {
            $payload['response'] = (!empty($results)) ? $results[0] : null;
        }

        return $payload;
    }

    private function execute($payload = [])
    {
        if(empty($this->methodName)){
            throw new ConnectorConfig('This connector requires a valid methodName for write ops');
        }
        if(empty($this->args[$this->idField])){
            throw new ConnectorArgs('This connector requires id for operations other than create');
        }
        $argsList = $this->args;
        if($this->methodName=='update'){
            unset($argsList[$this->idField]);
        }
        foreach ($argsList as $key=>$value){
            $type = isset($this->argsDefinition[$key]['type']) ? $this->argsDefinition[$key]['type'] : 'unknown';
            if($type=="Date"){
                $intermed=new \DateTime($value);
                $argsList[$key]=$intermed->format('Y-m-d H:i:s');
            } elseif ($type=="Boolean"&&$argsList[$key]===false){
                $argsList[$key]=0;
            }

        }
        switch ($this->methodName) {
            case 'create':
                    $this->client->insert($this->config['table'],$argsList);
                    $result=$this->client->fetchAssoc('SELECT * FROM '.$this->config['table'].' WHERE '.$this->idField.' = ?', array($this->args[$this->idField]));
                break;
            case 'update':
                    $this->client->update($this->config['table'],$argsList,[$this->idField=>$this->args[$this->idField]]);
                    $result=$this->client->fetchAssoc('SELECT * FROM '.$this->config['table'].' WHERE '.$this->idField.' = ?', array($this->args[$this->idField]));

                break;
            case 'upsert':
                    $existing=$this->client->fetchAssoc('SELECT * FROM '.$this->config['table'].' WHERE '.$this->idField.' = ?', array($this->args[$this->idField]));
                    if(!empty($existing)){
                        unset($argsList[$this->idField]);
                        $this->client->update($this->config['table'],$argsList,[$this->idField=>$this->args[$this->idField]]);
                    } else {
                        $this->client->insert($this->config['table'],$argsList);
                    }
                    $result=$this->client->fetchAssoc('SELECT * FROM '.$this->config['table'].' WHERE '.$this->idField.' = ?', array($this->args[$this->idField]));

                break;
            case 'delete':
                    $this->client->delete($this->config['table'],[$this->idField=>$this->args[$this->idField]]);
                    $result=$this->args;
                break;
            case 'bypass':
                $result=$this->args;
                break;
        }
        $this->payload['response'] = $result;
        return $this->payload;

    }

    protected function getValueForType($field,$value,$operator=null){
        $type = isset($this->argsDefinition[$field]['type']) ? $this->argsDefinition[$field]['type'] : 'unknown';
        switch ($type) {
            case 'Int':
            case 'Float':
            case 'Boolean':
                return $value;
                break;
            case 'Date':
                return new \DateTime($value);
                break;
            case 'String':
            case 'ID':
            case 'unknown':
            default:
                return $operator&&($operator=='like'||$operator=='notLike') ? "\"%$value%\"" : "\"$value\"" ;
                break;
        }
    }
}
