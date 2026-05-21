import { useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import MessageBubble from './MessageBubble';
import WelcomeScreen from './WelcomeScreen';
import useChatStore from '../../store/useChatStore';

export default function ChatWindow({ onPrompt, onRegenerate }) {
  const chat = useChatStore(s => s.getActiveChat());
  const bottomRef = useRef(null);
  const messages = chat?.messages ?? [];

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages.length, messages[messages.length - 1]?.streaming]);

  return (
    <div className="flex-1 overflow-y-auto scrollbar-none" id="chatScroll">
      <div className="mx-auto min-h-full max-w-5xl px-4 py-6">
        {messages.length === 0
          ? <WelcomeScreen onPrompt={onPrompt} />
          : (
            <AnimatePresence initial={false}>
              {messages.map((msg) => (
                <motion.div
                  key={msg.id}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.25 }}
                >
                  <MessageBubble
                    message={msg}
                    chatId={chat.id}
                    onRegenerate={() => onRegenerate(msg.id)}
                  />
                </motion.div>
              ))}
            </AnimatePresence>
          )
        }
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
