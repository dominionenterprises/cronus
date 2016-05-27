<?php
/**
 * Defines the ProcessRegistry class which uses MongoDB as a backend.
 */

namespace DominionEnterprises\Cronus;

/**
 * Class that adds/removes from a process registry.
 */
final class ProcessRegistry
{
    /** example doc:
     * {
     *     '_id': 'a unique id',
     *     'hosts': {
     *         'a hostname' : {
     *             'a pid': \MongoDB\BSON\UTCDateTime(expire time),
     *             ...
     *         },
     *         ...
     *     },
     *     'version' => \MongoDB\BSON\ObjectID(an id),
     * }
     */

    const MONGO_INT32_MAX = 2147483647;//2147483648 can overflow in php mongo without using the MongoInt64

    /**
     * Add to process registry. Adds based on $maxGlobalProcesses and $maxHostProcesses after a process registry cleaning.
     *
     * @param \MongoDB\Collection $collection the collection
     * @param string $id a unique id
     * @param int $minsBeforeExpire number of minutes before a process is considered expired.
     * @param int $maxGlobalProcesses max processes of an id allowed to run across all hosts.
     * @param int $maxHostProcesses max processes of an id allowed to run across a single host.
     *
     * @return boolean true if the process was added, false if not or there is too much concurrency at the moment.
     *
     * @throws \InvalidArgumentException if $id was not a string
     * @throws \InvalidArgumentException if $minsBeforeExpire was not an int
     * @throws \InvalidArgumentException if $maxGlobalProcesses was not an int
     * @throws \InvalidArgumentException if $maxHostProcesses was not an int
     */
    public static function add(
        \MongoDB\Collection $collection,
        $id,
        $minsBeforeExpire = PHP_INT_MAX,
        $maxGlobalProcesses = 1,
        $maxHostProcesses = 1
    )
    {
        if (!is_string($id)) {
            throw new \InvalidArgumentException('$id was not a string');
        }

        if (!is_int($minsBeforeExpire)) {
            throw new \InvalidArgumentException('$minsBeforeExpire was not an int');
        }

        if (!is_int($maxGlobalProcesses)) {
            throw new \InvalidArgumentException('$maxGlobalProcesses was not an int');
        }

        if (!is_int($maxHostProcesses)) {
            throw new \InvalidArgumentException('$maxHostProcesses was not an int');
        }

        $thisHostName = self::_getEncodedHostname();
        $thisPid = getmypid();

        //loop in case the update fails its optimistic concurrency check
        for ($i = 0; $i < 5; ++$i) {
            $collection->findOneAndUpdate(
                ['_id' => $id],
                ['$setOnInsert' => ['hosts' => [], 'version' => new \MongoDB\BSON\ObjectID()]],
                ['upsert' => true]
            );
            $existing = $collection->findOne(['_id' => $id], ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]);

            $replacement = $existing;
            $replacement['version'] = new \MongoDB\BSON\ObjectID();

            //clean $replacement based on their pids and expire times
            foreach ($existing['hosts'] as $hostname => $pids) {
                foreach ($pids as $pid => $expires) {
                    //our machine and not running
                    //the task expired
                    //our machine and pid is recycled (should rarely happen)
                    if (
                        ($hostname === $thisHostName && !file_exists("/proc/{$pid}"))
                        || time() >= $expires->toDateTime()->getTimestamp()
                        || ($hostname === $thisHostName && $pid === $thisPid)
                    ) {
                        unset($replacement['hosts'][$hostname][$pid]);
                    }
                }

                if (empty($replacement['hosts'][$hostname])) {
                    unset($replacement['hosts'][$hostname]);
                }
            }

            $totalPidCount = 0;
            foreach ($replacement['hosts'] as $hostname => $pids) {
                $totalPidCount += count($pids);
            }

            $thisHostPids = array_key_exists($thisHostName, $replacement['hosts']) ? $replacement['hosts'][$thisHostName] : [];

            if ($totalPidCount >= $maxGlobalProcesses || count($thisHostPids) >= $maxHostProcesses) {
                return false;
            }

            // add our process
            $expireSecs = time() + $minsBeforeExpire * 60;
            if (!is_int($expireSecs)) {
                if ($minsBeforeExpire > 0) {
                    $expireSecs = self::MONGO_INT32_MAX;
                } else {
                    $expireSecs = 0;
                }
            }

            $thisHostPids[$thisPid] = new \MongoDB\BSON\UTCDateTime($expireSecs * 1000);
            $replacement['hosts'][$thisHostName] = $thisHostPids;

            $status = $collection->replaceOne(
                ['_id' => $existing['_id'], 'version' => $existing['version']],
                $replacement,
                ['writeConcern' => new \MongoDB\Driver\WriteConcern(1, 100, true)]
            );
            if ($status->getMatchedCount() === 1) {
                return true;
            }

            //@codeCoverageIgnoreStart
            //hard to test the optimistic concurrency check
        }

        //too much concurrency at the moment, return false to signify not added.
        return false;
        //@codeCoverageIgnoreEnd
    }

    /**
     * Removes from process registry. Does not do anything needed for use of the add() method. Most will only use at the end of their script
     * so the mongo collection is up to date.
     *
     * @param \MongoDB\Collection $collection the collection
     * @param string $id a unique id
     *
     * @return void
     *
     * @throws \InvalidArgumentException if $id was not a string
     */
    public static function remove(\MongoDB\Collection $collection, $id)
    {
        if (!is_string($id)) {
            throw new \InvalidArgumentException('$id was not a string');
        }

        $thisHostName = self::_getEncodedHostname();
        $thisPid = getmypid();

        $collection->updateOne(
            ['_id' => $id],
            ['$unset' => ["hosts.{$thisHostName}.{$thisPid}" => ''], '$set' => ['version' => new \MongoDB\BSON\ObjectID()]]
        );
    }

    /**
     * Reset a process expire time in the registry.
     *
     * @param \MongoDB\Collection $collection the collection
     * @param string $id a unique id
     * @param int $minsBeforeExpire number of minutes before a process is considered expired.
     *
     * @return void
     *
     * @throws \InvalidArgumentException if $id was not a string
     * @throws \InvalidArgumentException if $minsBeforeExpire was not an int
     */
    public static function reset(\MongoDB\Collection $collection, $id, $minsBeforeExpire)
    {
        if (!is_string($id)) {
            throw new \InvalidArgumentException('$id was not a string');
        }

        if (!is_int($minsBeforeExpire)) {
            throw new \InvalidArgumentException('$minsBeforeExpire was not an int');
        }

        $expireSecs = time() + $minsBeforeExpire * 60;
        if (!is_int($expireSecs)) {
            if ($minsBeforeExpire > 0) {
                $expireSecs = self::MONGO_INT32_MAX;
            } else {
                $expireSecs = 0;
            }
        }

        $thisHostName = self::_getEncodedHostname();
        $thisPid = getmypid();

        $collection->updateOne(
            ['_id' => $id],
            [
                '$set' => [
                    "hosts.{$thisHostName}.{$thisPid}" => new \MongoDB\BSON\UTCDateTime($expireSecs * 1000),
                    'version' => new \MongoDB\BSON\ObjectID(),
                ],
            ]
        );
    }

    /**
     * Encodes '.' and '$' to be used as a mongo field name.
     *
     * @return string the encoded hostname from gethostname().
     */
    private static function _getEncodedHostname()
    {
        return str_replace(['.', '$'], ['_DOT_', '_DOLLAR_'], gethostname());
    }
}
