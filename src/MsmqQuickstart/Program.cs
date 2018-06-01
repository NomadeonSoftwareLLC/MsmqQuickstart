// Copyright (C) 2018 by Nomadeon Software LLC
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Messaging;
using System.Threading;

namespace MsmqQuickstart
{
    /// <summary>
    /// Quick start examples of interacting with MSMQ
    /// Requires MSMQ to be installed
    /// 
    /// Not intended as 100% production ready code
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Type message and press enter. Enter a blank line to stop");
            
            NotTransactionalExample();
            //TransactionalExample();
        }

        static void NotTransactionalExample()
        {
            // Syntax for private queue on local machine
            const string path = @".\private$\nonTransactionalExample";

            // Initialize queue, create if it does not exist
            MessageQueue queue = null;
            if (!MessageQueue.Exists(path))
            {
                queue = MessageQueue.Create(path, transactional: false);
            }
            else
            {
                queue = new MessageQueue(path);
            }

            Console.WriteLine("Queue connected: " + path);

            // Setup serializer
            queue.Formatter = new XmlMessageFormatter(new Type[] { typeof(string) });

            // Create thread for reading from queue
            bool keepRunning = true;
            ThreadStart threadStart = new ThreadStart(() => {
                while (keepRunning)
                {
                    Message message = queue.Receive();
                    Console.WriteLine("Received: " + message.Body);

                    // Sleep for a bit to allow us to enter multiple records
                    Thread.Sleep((int)TimeSpan.FromSeconds(1).TotalMilliseconds);
                }
            });
            Thread readThread = new Thread(threadStart);
            readThread.IsBackground = true; // Allows process to exit if thread is halted on queue.Receive()
            readThread.Start();

            // Read console lines and serialize into queue
            string input = Console.ReadLine();
            while (input != String.Empty)
            {
                queue.Send(new Message(input));
                input = Console.ReadLine();
            }

            // Give thread a chance to close down nicely
            Console.WriteLine("Closing down...");
            keepRunning = false;
            bool exited = readThread.Join(TimeSpan.FromSeconds(2));
            if (!exited)
            {
                readThread.Abort();
            }
        }

        static void TransactionalExample()
        {
            // Syntax for private queue on local machine
            const string path = @".\private$\transactionalExample";

            // Initialize queue, create if it does not exist
            MessageQueue queue = null;
            if (!MessageQueue.Exists(path))
            {
                queue = MessageQueue.Create(path, transactional: true);
            }
            else
            {
                queue = new MessageQueue(path);
            }

            Console.WriteLine("Queue connected: " + path);

            // Setup serializer
            queue.Formatter = new XmlMessageFormatter(new Type[] { typeof(string) });

            // Create thread for reading from queue
            bool keepRunning = true;
            ThreadStart threadStart = new ThreadStart(() => {
                while (keepRunning)
                {
                    MessageQueueTransaction transaction = new MessageQueueTransaction();
                    transaction.Begin();

                    try
                    {
                        Message message = queue.Receive(transaction);
                        Console.WriteLine("Received: " + message.Body);

                        // Do something that could fail (i.e. network call)

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Abort();
                        Console.WriteLine(ex);
                    }

                    // Sleep for a bit to allow us to enter multiple records
                    Thread.Sleep((int)TimeSpan.FromSeconds(1).TotalMilliseconds);
                }
            });
            Thread readThread = new Thread(threadStart);
            readThread.IsBackground = true; // Allows process to exit if thread is halted on queue.Receive()
            readThread.Start();

            // Read console lines and serialize into queue
            string input = Console.ReadLine();
            while (input != String.Empty)
            {
                // Although this transaction may not be needed by program logic, you 
                // must include it if queue is transactional. Otherwise Send will silently fail.
                MessageQueueTransaction transaction = new MessageQueueTransaction();
                transaction.Begin();

                queue.Send(new Message(input), transaction);
                transaction.Commit();

                input = Console.ReadLine();
            }

            // Give thread a chance to close down nicely
            Console.WriteLine("Closing down...");
            keepRunning = false;
            bool exited = readThread.Join(TimeSpan.FromSeconds(2));
            if (!exited)
            {
                readThread.Abort();
            }
        }
    }
}
