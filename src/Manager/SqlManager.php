<?php

namespace Simplon\Postgres\Manager;

use Simplon\Postgres\Postgres;
use Simplon\Postgres\PostgresQueryIterator;

class SqlManager
{
    /** @var Postgres */
    protected $dbInstance;

    /**
     * @param Postgres $mysqlInstance
     */
    public function __construct(Postgres $mysqlInstance)
    {
        $this->dbInstance = $mysqlInstance;
    }

    /**
     * @return Postgres
     */
    protected function getDbInstance()
    {
        return $this->dbInstance;
    }

    /**
     * @return bool|int
     */
    public function getRowCount()
    {
        return $this
            ->getDbInstance()
            ->getRowCount();
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return bool
     */
    public function executeSql(SqlQueryBuilder $sqlBuilder)
    {
        return $this
            ->getDbInstance()
            ->executeSql($sqlBuilder->getQuery());
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return bool|string
     */
    public function fetchColumn(SqlQueryBuilder $sqlBuilder)
    {
        $result = $this
            ->getDbInstance()
            ->fetchColumn($sqlBuilder->getQuery(), $sqlBuilder->getConditions());

        if ($result !== null)
        {
            return (string)$result;
        }

        return false;
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return array|bool
     */
    public function fetchColumnMany(SqlQueryBuilder $sqlBuilder)
    {
        $result = $this
            ->getDbInstance()
            ->fetchColumnMany($sqlBuilder->getQuery(), $sqlBuilder->getConditions());

        if ($result !== null)
        {
            return (array)$result;
        }

        return false;
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return PostgresQueryIterator
     */
    public function fetchColumnManyCursor(SqlQueryBuilder $sqlBuilder)
    {
        return $this
            ->getDbInstance()
            ->fetchColumnManyCursor($sqlBuilder->getQuery(), $sqlBuilder->getConditions());
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return array|bool
     */
    public function fetchRow(SqlQueryBuilder $sqlBuilder)
    {
        $result = $this
            ->getDbInstance()
            ->fetchRow($sqlBuilder->getQuery(), $sqlBuilder->getConditions());

        if ($result !== null)
        {
            return (array)$result;
        }

        return false;
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return array|bool
     */
    public function fetchRowMany(SqlQueryBuilder $sqlBuilder)
    {
        $result = $this
            ->getDbInstance()
            ->fetchRowMany($sqlBuilder->getQuery(), $sqlBuilder->getConditions());

        if ($result !== null)
        {
            return (array)$result;
        }

        return false;
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return PostgresQueryIterator
     */
    public function fetchRowManyCursor(SqlQueryBuilder $sqlBuilder)
    {
        return $this
            ->getDbInstance()
            ->fetchRowManyCursor($sqlBuilder->getQuery(), $sqlBuilder->getConditions());
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return array|null
     */
    public function insert(SqlQueryBuilder $sqlBuilder)
    {
        if ($sqlBuilder->hasMultiData())
        {
            return $this->getDbInstance()
                ->insertMany(
                    $sqlBuilder->getTableName(),
                    $sqlBuilder->getData(),
                    $sqlBuilder->hasInsertIgnore()
                );
        }

        return $this->getDbInstance()
            ->insert(
                $sqlBuilder->getTableName(),
                $sqlBuilder->getData(),
                $sqlBuilder->hasInsertIgnore()
            );
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return array|null
     */
    public function replace(SqlQueryBuilder $sqlBuilder)
    {
        if ($sqlBuilder->hasMultiData())
        {
            return $this->getDbInstance()
                ->replaceMany(
                    $sqlBuilder->getTableName(),
                    $sqlBuilder->getData()
                );
        }

        return $this->getDbInstance()
            ->replace(
                $sqlBuilder->getTableName(),
                $sqlBuilder->getData()
            );
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return bool
     */
    public function update(SqlQueryBuilder $sqlBuilder)
    {
        return $this->getDbInstance()
            ->update(
                $sqlBuilder->getTableName(),
                $sqlBuilder->getConditions(),
                $sqlBuilder->getData(),
                $sqlBuilder->getConditionsQuery()
            );
    }

    /**
     * @param SqlQueryBuilder $sqlBuilder
     *
     * @return bool
     */
    public function delete(SqlQueryBuilder $sqlBuilder)
    {
        return $this->getDbInstance()
            ->delete(
                $sqlBuilder->getTableName(),
                $sqlBuilder->getConditions(),
                $sqlBuilder->getConditionsQuery()
            );
    }
}
