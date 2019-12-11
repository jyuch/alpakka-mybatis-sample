package dev.jyuch.alpakka.mybatis.sample;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.IOResult;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import dev.jyuch.alpakka.mybatis.javadsl.MyBatis;
import dev.jyuch.alpakka.mybatis.sample.model.User;
import dev.jyuch.alpakka.mybatis.sample.service.UserMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;

public class App {
    public static void main(String[] args) throws Exception {
        // この辺はまぁおまじないだよね
        ActorSystem system = ActorSystem.create();
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(Resources.getResourceAsStream("mybatis-config.xml"));

        // セッションをアプリケーションのライフサイクルの間保持しておく
        // インメモリの場合H2の仕様上最後のセッションが閉じられた時点でデータがすべて消える
        SqlSession sessionHolder = sqlSessionFactory.openSession();

        // ActorSystem停止時にセッションを閉じる
        system.registerOnTermination(sessionHolder::close);

        // サンプル用にDBを初期化する
        UserMapper mapper = sessionHolder.getMapper(UserMapper.class);
        mapper.initialize();
        sessionHolder.commit();

        source(system, sqlSessionFactory);
        flow(system, sqlSessionFactory);
        sink(system, sqlSessionFactory);
        system.terminate();
    }

    public static void source(ActorSystem system, SqlSessionFactory sqlSessionFactory) throws Exception {
        Source<User, CompletionStage<IOResult>> source = MyBatis.source(
                sqlSessionFactory::openSession,
                sqlSession -> sqlSession.getMapper(UserMapper.class).select()
        );

        /*
         * User{id=1, name='alice'}
         * User{id=2, name='bob'}
         */
        CompletionStage<Done> onComplete = source.runForeach(System.out::println, system);
        onComplete.toCompletableFuture().get();
    }

    public static void flow(ActorSystem system, SqlSessionFactory sqlSessionFactory) throws Exception {
        Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2));
        Flow<Integer, String, CompletionStage<IOResult>> flow = MyBatis.flow(
                sqlSessionFactory::openSession,
                (session, i) -> {
                    UserMapper mapper = session.getMapper(UserMapper.class);
                    User user = mapper.selectById(i);
                    return user.getName();
                },
                false
        );

        /*
         * alice
         * bob
         */
        CompletionStage<Done> onComplete = source.via(flow).runForeach(System.out::println, system);
        onComplete.toCompletableFuture().get();
    }

    public static void sink(ActorSystem system, SqlSessionFactory sqlSessionFactory) throws Exception {
        Source<User, NotUsed> source = Source.from(
                Arrays.asList(
                        new User(3, "Carol"),
                        new User(3, "Dave")));
        Sink<User, CompletionStage<IOResult>> sink = MyBatis.sink(
                sqlSessionFactory::openSession,
                (session, it) -> {
                    UserMapper mapper = session.getMapper(UserMapper.class);
                    mapper.insert(it);
                },
                true
        );
        CompletionStage<IOResult> onComplete = source.toMat(sink, Keep.right()).run(system);
        onComplete.toCompletableFuture().get();

        SqlSession session = sqlSessionFactory.openSession();
        UserMapper mapper = session.getMapper(UserMapper.class);

        /*
         * User{id=1, name='alice'}
         * User{id=2, name='bob'}
         * User{id=3, name='Carol'}
         * User{id=3, name='Dave'}
         */
        for (User it : mapper.select()) {
            System.out.println(it);
        }
    }
}
