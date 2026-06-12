import { useEffect, useRef, useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ArrowDown } from 'lucide-react';
import MessageBubble from './MessageBubble';
import WelcomeScreen from './WelcomeScreen';
import useChatStore from '../../store/useChatStore';

export default function ChatWindow({ onPrompt, onRegenerate }) {
  const chat = useChatStore(s => s.getActiveChat());
  const bottomRef = useRef(null);
  const scrollRef = useRef(null);
  const [showScrollBtn, setShowScrollBtn] = useState(false);
  const messages = chat?.messages ?? [];

  // Auto-scroll to bottom on new messages / streaming
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages.length, messages[messages.length - 1]?.streaming]);

  // Show scroll-to-bottom button when user scrolls up
  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    const distFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    setShowScrollBtn(distFromBottom > 200);
  }, []);

  const scrollToBottom = useCallback(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  return (
    <div
      ref={scrollRef}
      onScroll={handleScroll}
      className="flex-1 overflow-y-auto scrollbar-none relative"
      id="chatScroll"
    >
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

      {/* Scroll-to-bottom FAB */}
      <AnimatePresence>
        {showScrollBtn && messages.length > 0 && (
          <motion.button
            initial={{ opacity: 0, scale: 0.8, y: 10 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.8, y: 10 }}
            onClick={scrollToBottom}
            className="fixed bottom-36 right-6 z-20 w-9 h-9 rounded-full flex items-center justify-center shadow-xl transition-colors"
            style={{
              background: 'var(--surface-3)',
              border: '1px solid var(--border-2)',
              backdropFilter: 'blur(12px)',
            }}
            whileHover={{ scale: 1.1 }}
            whileTap={{ scale: 0.9 }}
            aria-label="Scroll to bottom"
          >
            <ArrowDown size={14} className="text-t2" />
          </motion.button>
        )}
      </AnimatePresence>
    </div>
  );
}
