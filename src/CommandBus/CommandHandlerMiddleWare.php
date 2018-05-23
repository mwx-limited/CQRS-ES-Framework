<?php
namespace SmoothPhp\CommandBus;

use SmoothPhp\Contracts\CommandBus\Command;
use SmoothPhp\Contracts\CommandBus\CommandBusMiddleware;
use SmoothPhp\Contracts\CommandBus\CommandHandlerResolver;
use SmoothPhp\Contracts\CommandBus\CommandTranslator;

/**
 * Class PlainCommandBus
 * @package SmoothPhp\CommandBus
 * @author Simon Bennett <simon@bennett.im>
 */
final class CommandHandlerMiddleWare implements CommandBusMiddleware
{
    const ATTEMPTS = 4;
    const ATTEMPT_INTERVAL = 100;

    /** @var CommandTranslator */
    private $commandTranslator;

    /** @var CommandHandlerResolver */
    private $handlerResolver;

    /**
     * @param CommandTranslator $commandTranslator
     * @param CommandHandlerResolver $handlerResolver
     */
    public function __construct(CommandTranslator $commandTranslator, CommandHandlerResolver $handlerResolver)
    {
        $this->commandTranslator = $commandTranslator;
        $this->handlerResolver = $handlerResolver;
    }

    /**
     * @param Command $command
     * @param callable $next
     * @return mixed
     * @throws \Exception
     */
    public function execute(Command $command, callable $next)
    {
        $handler = $this->commandTranslator->toCommandHandler($command);

        // It is possible to have contention here when an aggregate is updated simultaneously on
        // two separate threads. Both will read the existing events at the same time, then add new
        // events and try to save with the updated play-head. Since both will increment the play-head
        // by one, the loser will receive a duplicate on unique key exception. Since the command that
        // succeeded might have change the aggregate in a way that would affect the business logic of
        // the second it has to start from scratch, loading all events including the one(s) just saved.

        $attempt = 0;
        $lastError = null;

        while ($attempt++ < self::ATTEMPTS) {
            try {
                return $this->handlerResolver->make($handler)->handle($command);
            } catch (\Exception $exception) {
                $lastError = $exception;
                if ($attempt < self::ATTEMPTS) {
                    usleep(self::ATTEMPT_INTERVAL * $attempt); // linearly increasing back-off
                } else {
                    throw $exception;
                }
            }
        }

        throw $lastError;
    }
}
