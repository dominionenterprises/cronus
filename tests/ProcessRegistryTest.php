<?php

namespace DominionEnterprises\Cronus;

//encoded hostname from the replaced builtin function
const HOSTNAME = 'my_DOT_host_DOLLAR_name';

/**
 * @coversDefaultClass \DominionEnterprises\Cronus\ProcessRegistry
 * @covers ::<private>
 */
final class ProcessRegistryTest extends \PHPUnit_Framework_TestCase
{
    private $_collection;

    public function setUp()
    {
        $mongo = new \MongoClient();
        $this->_collection = $mongo->selectDB('testing')->selectCollection('processes');
        $this->_collection->drop();
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_emptyCollection()
    {
        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId'));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_existingDifferentHost()
    {
        $expireSecs = time() + 60;
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => ['different host' => ['a pid' => new \MongoDate($expireSecs)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId', PHP_INT_MAX, 2, 1));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => ['different host' => ['a pid' => $expireSecs], HOSTNAME => [getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];
        $result['hosts']['different host']['a pid'] = $result['hosts']['different host']['a pid']->sec;
        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_overMaxGlobalProcessesOnDifferentHost()
    {
        $initalTask = [
            '_id' => 'testId',
            'hosts' => ['different host' => ['a pid' => new \MongoDate(time() + 60)]],
            'version' => new \MongoId(),
        ];

        $this->_collection->insert($initalTask);

        $this->assertFalse(ProcessRegistry::add($this->_collection, 'testId', PHP_INT_MAX, 1, PHP_INT_MAX));
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_overMaxGlobalProcessesOnSameHost()
    {
        $pipes = [];
        $process = proc_open('sleep 3 &', self::_getDevNullProcOpenDescriptors(), $pipes);
        $status = proc_get_status($process);

        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [$status['pid'] => new \MongoDate(time() + 60)]],
            'version' => new \MongoId(),
        ];

        $this->_collection->insert($initalTask);

        $this->assertFalse(ProcessRegistry::add($this->_collection, 'testId', PHP_INT_MAX, 1, PHP_INT_MAX));
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_overMaxHostProcesses()
    {
        $pipes = [];
        $process = proc_open('sleep 3 &', self::_getDevNullProcOpenDescriptors(), $pipes);
        $status = proc_get_status($process);

        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [$status['pid'] => new \MongoDate(time() + 60)]],
            'version' => new \MongoId(),
        ];

        $this->_collection->insert($initalTask);

        $this->assertFalse(ProcessRegistry::add($this->_collection, 'testId', PHP_INT_MAX, PHP_INT_MAX, 1));
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_cleaningNotRunningProcessWithoutExtra()
    {
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => ['a pid' => new \MongoDate(time() + 60)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId'));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_cleaningNotRunningProcessWithExtra()
    {
        $pipes = [];
        $process = proc_open('sleep 3 &', self::_getDevNullProcOpenDescriptors(), $pipes);
        $status = proc_get_status($process);
        $extraPid = $status['pid'];

        $expireSecs = time() + 60;
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [$extraPid => new \MongoDate($expireSecs), 'a pid' => new \MongoDate($expireSecs)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId', PHP_INT_MAX, 2, 2));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [$extraPid => $expireSecs, getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts'][HOSTNAME][$extraPid] = $result['hosts'][HOSTNAME][$extraPid]->sec;
        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_cleaningExpiredProcessWithoutExtra()
    {
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => ['different host' => ['a pid' => new \MongoDate(time() - 1)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId'));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_cleaningExpiredProcessWithExtra()
    {
        $expireSecs = time() + 60;
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => ['different host' => ['expiring pid' => new \MongoDate(time() - 1), 'another pid' => new \MongoDate($expireSecs)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId', PHP_INT_MAX, 2, 2));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => ['different host' => ['another pid' => $expireSecs], HOSTNAME => [getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts']['different host']['another pid'] = $result['hosts']['different host']['another pid']->sec;
        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_cleaningRecycledProcessWithoutExtra()
    {
        $initialVersion = new \MongoId();
        $initalTask = ['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => new \MongoDate(time() + 60)]], 'version' => $initialVersion];

        $this->_collection->insert($initalTask);

        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId'));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_cleaningRecycledProcessWithExtra()
    {
        $pipes = [];
        $process = proc_open('sleep 3 &', self::_getDevNullProcOpenDescriptors(), $pipes);
        $status = proc_get_status($process);
        $extraPid = $status['pid'];

        $expireSecs = time() + 60;
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [$extraPid => new \MongoDate($expireSecs), getmypid() => new \MongoDate($expireSecs)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId', PHP_INT_MAX, 2, 2));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [$extraPid => $expireSecs, getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts'][HOSTNAME][$extraPid] = $result['hosts'][HOSTNAME][$extraPid]->sec;
        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::add
     */
    public function add_underflowMinsBeforeExpire()
    {
        $this->assertTrue(ProcessRegistry::add($this->_collection, 'testId', ~PHP_INT_MAX));

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame(['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => 0]], 'version' => $result['version']], $result);
    }

    /**
     * @test
     * @covers ::add
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage $id was not a string
     */
    public function add_nonStringId()
    {
        ProcessRegistry::add($this->_collection, true);
    }

    /**
     * @test
     * @covers ::add
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage $minsBeforeExpire was not an int
     */
    public function add_nonIntMinsBeforeExpire()
    {
        ProcessRegistry::add($this->_collection, 'not under test', true);
    }

    /**
     * @test
     * @covers ::add
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage $maxGlobalProcesses was not an int
     */
    public function add_nonIntMaxGlobalProcesses()
    {
        ProcessRegistry::add($this->_collection, 'not under test', PHP_INT_MAX, true);
    }

    /**
     * @test
     * @covers ::add
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage $maxHostProcesses was not an int
     */
    public function add_nonIntMaxHostProcesses()
    {
        ProcessRegistry::add($this->_collection, 'not under test', PHP_INT_MAX, 1, true);
    }

    /**
     * @test
     * @covers ::remove
     */
    public function remove_withExistingProcess()
    {
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => ['a pid' => new \MongoDate(0), getmypid() => new \MongoDate(time() + 60)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        ProcessRegistry::remove($this->_collection, 'testId');

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $result['hosts'][HOSTNAME]['a pid'] = $result['hosts'][HOSTNAME]['a pid']->sec;
        $this->assertSame(['_id' => 'testId', 'hosts' => [HOSTNAME => ['a pid' => 0]], 'version' => $result['version']], $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::remove
     */
    public function remove_withoutExistingProcess()
    {
        $initialVersion = new \MongoId();
        $initalTask = ['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => new \MongoDate(time() + 60)]], 'version' => $initialVersion];

        $this->_collection->insert($initalTask);

        ProcessRegistry::remove($this->_collection, 'testId');

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $this->assertSame(['_id' => 'testId', 'hosts' => [HOSTNAME => []], 'version' => $result['version']], $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::remove
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage $id was not a string
     */
    public function remove_nonStringId()
    {
        ProcessRegistry::remove($this->_collection, true);
    }

    /**
     * @test
     * @covers ::reset
     */
    public function reset_withoutExtra()
    {
        $initialExpireSecs = time() + 60;
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [getmypid() => new \MongoDate($initialExpireSecs)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        ProcessRegistry::reset($this->_collection, 'testId', 2);

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $this->assertGreaterThan($initialExpireSecs, $result['hosts'][HOSTNAME][getmypid()]->sec);
        $this->assertLessThanOrEqual(time() + 120, $result['hosts'][HOSTNAME][getmypid()]->sec);
        $result['hosts'][HOSTNAME][getmypid()] = null;

        $this->assertSame(['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => null]], 'version' => $result['version']], $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::reset
     */
    public function reset_withExtra()
    {
        $initialExpireSecs = time() + 60;
        $initialVersion = new \MongoId();
        $initalTask = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [getmypid() => new \MongoDate($initialExpireSecs), 'extra pid' => new \MongoDate(0)]],
            'version' => $initialVersion,
        ];

        $this->_collection->insert($initalTask);

        ProcessRegistry::reset($this->_collection, 'testId', 2);

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $this->assertGreaterThan($initialExpireSecs, $result['hosts'][HOSTNAME][getmypid()]->sec);
        $this->assertLessThanOrEqual(time() + 120, $result['hosts'][HOSTNAME][getmypid()]->sec);
        $result['hosts'][HOSTNAME][getmypid()] = null;

        $expected = ['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => null, 'extra pid' => 0]], 'version' => $result['version']];

        $result['hosts'][HOSTNAME]['extra pid'] = $result['hosts'][HOSTNAME]['extra pid']->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::reset
     */
    public function reset_underflowMinsBeforeExpire()
    {
        $initialVersion = new \MongoId();
        $initalTask = ['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => new \MongoDate(time() + 60)]], 'version' => $initialVersion];

        $this->_collection->insert($initalTask);

        ProcessRegistry::reset($this->_collection, 'testId', ~PHP_INT_MAX);

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame(['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => 0]], 'version' => $result['version']], $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::reset
     */
    public function reset_overflowMinsBeforeExpire()
    {
        $initialVersion = new \MongoId();
        $initalTask = ['_id' => 'testId', 'hosts' => [HOSTNAME => [getmypid() => new \MongoDate(time() + 60)]], 'version' => $initialVersion];

        $this->_collection->insert($initalTask);

        ProcessRegistry::reset($this->_collection, 'testId', PHP_INT_MAX);

        $this->assertSame(1, $this->_collection->count());
        $result = $this->_collection->findOne();

        $expected = [
            '_id' => 'testId',
            'hosts' => [HOSTNAME => [getmypid() => ProcessRegistry::MONGO_INT32_MAX]],
            'version' => $result['version'],
        ];

        $result['hosts'][HOSTNAME][getmypid()] = $result['hosts'][HOSTNAME][getmypid()]->sec;
        $this->assertSame($expected, $result);
        $this->assertNotSame((string)$initialVersion, (string)$result['version']);
    }

    /**
     * @test
     * @covers ::reset
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage $id was not a string
     */
    public function reset_nonStringId()
    {
        ProcessRegistry::reset($this->_collection, true, 0);
    }

    /**
     * @test
     * @covers ::reset
     * @expectedException InvalidArgumentException
     * @expectedExceptionMessage $minsBeforeExpire was not an int
     */
    public function reset_nonIntMinsBeforeExpire()
    {
        ProcessRegistry::reset($this->_collection, 'testId', true);
    }

    private static function _getDevNullProcOpenDescriptors()
    {
        return [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']];
    }
}

function gethostname()
{
    return 'my.host$name';
}
