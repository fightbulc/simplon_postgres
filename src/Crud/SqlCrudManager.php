<?php

namespace Simplon\Postgres\Crud;

use Simplon\Postgres\Postgres;
use Simplon\Postgres\PostgresException;

/**
 * SqlCrudManager
 * @package Simplon\Postgres\Crud
 * @author Tino Ehrich (tino@bigpun.me)
 */
class SqlCrudManager
{
    /** @var \Simplon\Postgres\Postgres */
    protected $dbInstance;

    /**
     * @param Postgres $mysql
     */
    public function __construct(Postgres $mysql)
    {
        $this->dbInstance = $mysql;
    }

    /**
     * @return Postgres
     */
    protected function getDbInstance()
    {
        return $this->dbInstance;
    }

    /**
     * @param array $conds
     * @param null $condsQuery
     *
     * @return string
     */
    protected function getCondsQuery(array $conds, $condsQuery = null)
    {
        if ($condsQuery !== null)
        {
            return (string)$condsQuery;
        }

        $condsString = array();

        foreach ($conds as $key => $val)
        {
            $query = $key . ' = :' . $key;

            if (is_array($val) === true)
            {
                $query = $key . ' IN (:' . $key . ')';
            }

            $condsString[] = $query;
        }

        return join(' AND ', $condsString);
    }

    /**
     * @param SqlCrudInterface $sqlCrudInterface
     *
     * @return array
     */
    protected function getData(SqlCrudInterface &$sqlCrudInterface)
    {
        $data = array();

        foreach ($sqlCrudInterface->crudColumns() as $variable => $column)
        {
            $methodName = 'get' . ucfirst($variable);
            $data[$column] = $sqlCrudInterface->$methodName();
        }

        return $data;
    }

    /**
     * @param SqlCrudInterface $sqlCrudInterface
     * @param array $data
     *
     * @return SqlCrudInterface
     */
    protected function setData(SqlCrudInterface $sqlCrudInterface, array $data)
    {
        $columns = array_flip($sqlCrudInterface->crudColumns());

        foreach ($data as $column => $value)
        {
            if (isset($columns[$column]))
            {
                $methodName = 'set' . ucfirst($columns[$column]);
                $sqlCrudInterface->$methodName($value);
            }
        }

        return $sqlCrudInterface;
    }

    /**
     * @param SqlCrudInterface $sqlCrudInterface
     * @param bool $insertIgnore
     *
     * @return bool|SqlCrudInterface
     * @throws PostgresException
     */
    public function create(SqlCrudInterface $sqlCrudInterface, $insertIgnore = false)
    {
        // do something before we save
        $sqlCrudInterface->crudBeforeSave(true);

        // save to db
        $insertId = $this->getDbInstance()->insert(
            $sqlCrudInterface->crudGetSource(),
            $this->getData($sqlCrudInterface),
            $insertIgnore
        );

        if ($insertId !== false)
        {
            // set id
            if (is_bool($insertId) !== true && method_exists($sqlCrudInterface, 'setId'))
            {
                $sqlCrudInterface->setId($insertId);
            }

            // do something after we saved
            $sqlCrudInterface->crudAfterSave(true);

            return $sqlCrudInterface;
        }

        return false;
    }

    /**
     * @param SqlCrudInterface $sqlCrudInterface
     * @param array $conds
     * @param null $sortBy
     * @param null $condsQuery
     *
     * @return bool|SqlCrudInterface
     */
    public function read(SqlCrudInterface $sqlCrudInterface, array $conds, $sortBy = null, $condsQuery = null)
    {
        // handle custom query
        $query = $sqlCrudInterface->crudGetQuery();

        // fallback to standard query
        if ($query === '')
        {
            $query = "SELECT * FROM {$sqlCrudInterface::crudGetSource()} WHERE {$this->getCondsQuery($conds, $condsQuery)}";
        }

        // add sorting
        if($sortBy !== null)
        {
            $query .= " ORDER BY {$sortBy}";
        }

        // fetch data
        $data = $this->getDbInstance()->fetchRow($query, $conds);

        if ($data !== false)
        {
            return $this->setData($sqlCrudInterface, $data);
        }

        return false;
    }

    /**
     * @param SqlCrudInterface $sqlCrudInterface
     * @param array $conds
     * @param null $sortBy
     * @param null $condsQuery
     *
     * @return bool|SqlCrudInterface[]
     */
    public function readMany(SqlCrudInterface $sqlCrudInterface, array $conds = array(), $sortBy = null, $condsQuery = null)
    {
        // handle custom query
        $query = $sqlCrudInterface->crudGetQuery();

        // fallback to standard query
        if ($query === '')
        {
            $query = "SELECT * FROM {$sqlCrudInterface::crudGetSource()}";
        }

        // add conds
        if (empty($conds) === false)
        {
            $query .= " WHERE {$this->getCondsQuery($conds, $condsQuery)}";
        }

        // add sorting
        if($sortBy !== null)
        {
            $query .= " ORDER BY {$sortBy}";
        }

        // fetch data
        $cursor = $this->getDbInstance()->fetchRowManyCursor($query, $conds);

        // build result
        $sqlCrudInterfaceMany = array();

        if ($cursor !== false)
        {
            foreach ($cursor as $data)
            {
                $sqlCrudInterfaceMany[] = $this->setData(clone $sqlCrudInterface, $data);
            }

            return empty($sqlCrudInterfaceMany) ? false : $sqlCrudInterfaceMany;
        }

        return false;
    }

    /**
     * @param SqlCrudInterface $sqlCrudInterface
     * @param array $conds
     * @param null $condsQuery
     *
     * @return bool|SqlCrudInterface
     * @throws \Simplon\Postgres\PostgresException
     */
    public function update(SqlCrudInterface $sqlCrudInterface, array $conds, $condsQuery = null)
    {
        // do something before we save
        $sqlCrudInterface->crudBeforeSave(false);

        $response = $this->getDbInstance()->update(
            $sqlCrudInterface::crudGetSource(),
            $conds,
            $this->getData($sqlCrudInterface),
            $this->getCondsQuery($conds, $condsQuery)
        );

        if ($response !== false)
        {
            // do something after update
            $sqlCrudInterface->crudAfterSave(false);

            return $sqlCrudInterface;
        }

        return false;
    }

    /**
     * @param $crudSource
     * @param array $conds
     * @param null $condsQuery
     *
     * @return bool
     */
    public function delete($crudSource, array $conds, $condsQuery = null)
    {
        return $this->getDbInstance()->delete(
            $crudSource,
            $conds,
            $this->getCondsQuery($conds, $condsQuery)
        );
    }
}