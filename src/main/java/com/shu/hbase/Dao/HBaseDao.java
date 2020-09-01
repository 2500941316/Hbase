package com.shu.hbase.Dao;

import com.shu.hbase.Dao.download.DownLoad;
import com.shu.hbase.Pojo.*;
import com.shu.hbase.Tools.HbasePool.HBase.HBaseConn;
import com.shu.hbase.Tools.HbasePool.HbaseConnectionPool;
import com.shu.hbase.Tools.HdfsPool.HdfsConnectionPool;
import com.shu.hbase.Tools.TableModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Stat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

public class HBaseDao {

    //将上传的文件名和新建的文件夹 checkTable方法中新建的文件插入files表中
    public static boolean insertToFiles(File localPath, String fileType, String hdfsPath, String backId, String uId, String fileId) {
        Connection hBaseConn = null;
        Table fileTable = null;
        if (!backId.substring(0, 8).equals(uId)) {
            return false;
        }
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));

            long l = System.currentTimeMillis();
            Put put = new Put(Bytes.toBytes(fileId));
            if (localPath != null) {
                put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_NAME), Bytes.toBytes(localPath.getName()));
                put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_SIZE), Bytes.toBytes(String.valueOf(localPath.length())));
                put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_ISDIR), Bytes.toBytes("false"));
            } else {
                String newPath = hdfsPath;
                newPath = newPath.substring(newPath.lastIndexOf("/") + 1);
                put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_NAME), Bytes.toBytes(newPath));
                put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_SIZE), Bytes.toBytes("-"));
                put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_ISDIR), Bytes.toBytes("true"));
            }
            //如果是首页的数据则back设为/+学号；如果不是首页的数据则back设为当前文件夹的id号
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_TYPE), Bytes.toBytes(fileType));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_BACK), Bytes.toBytes(backId));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_PATH), Bytes.toBytes(hdfsPath));

            //如果当前的backid不等于uid，说明在一个文件夹下面上传文件，则先查询文件夹的权限，然后等于该文件夹的权限
            if (!backId.equals(uId)) {
                //查询fileId是backId的文件夹的权限
                Get authGet = new Get(Bytes.toBytes(backId));
                authGet.setMaxVersions();
                authGet.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth));
                Result authResult = fileTable.get(authGet);
                List<String> authList = new ArrayList<>();
                if (!authResult.isEmpty()) {
                    for (Cell cell : authResult.rawCells()) {
                        if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.FILE_TABLE_Auth)) {
                            authList.add(Bytes.toString(CellUtil.cloneValue(cell)));
                        }
                    }
                }
                for (String auth : authList) {
                    put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth), ++l, Bytes.toBytes(auth));
                }
            } else {
                put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth), Bytes.toBytes(uId));
            }

            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_TIME), Bytes.toBytes(l + ""));
            fileTable.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HbaseConnectionPool.releaseConnection(hBaseConn);
        }
        return true;
    }


    //在用户登录的时候就index表有没有该用户，如果没有则初始化该用户,并检查分组表有无该用户的部门组，如果有则放入
    public static TableModel checkTables(String backId, String uid, String department) {
        Connection hBaseConn = null;
        Table fileTable = null;
        Table userTable = null;
        TableModel tableModel = new TableModel();
        if (StringUtils.isEmpty(uid) || StringUtils.isEmpty(department)) {
            return TableModel.error("参数有误");
        }
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            userTable = hBaseConn.getTable(TableName.valueOf(Static.USER_TABLE));
            //查询files表中有没有默认文件夹，如果没有则调用mkdir创建
            Scan scan = new Scan();

            FilterList filterList = new FilterList();
            Filter colFilter = new PrefixFilter(Bytes.toBytes(uid));
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    Static.FILE_TABLE_CF.getBytes(),
                    Static.FILE_TABLE_PATH.getBytes(),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("/shuwebfs/" + uid + "/我的文档"))); //安装hdfs的路径查询
            singleColumnValueFilter.setFilterIfMissing(true);
            filterList.addFilter(colFilter);
            filterList.addFilter(singleColumnValueFilter);
            scan.setFilter(filterList);
            ResultScanner scanner = fileTable.getScanner(scan);

            if (scanner.next() == null) {
                //直接创建文件夹即可
                HBaseDao.mkdir(backId, "我的文档", uid);
            }

            //查询user表中有没有用户rowkey，如果没有则插入
            if (!insertOrUpdateUser(userTable, "0", uid, "upload")) {
                return TableModel.error("文件超出存储容量");
            }

            List<FileInfoVO> fileInfoVOS = new ArrayList<>();
            FilterList filterListNew = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            Filter colFilterNew = new PrefixFilter(Bytes.toBytes("00000000"));
            Scan newScan = new Scan();
            newScan.setReversed(true);
            newScan.setCaching(1);
            Filter filter = new PageFilter(7);
            filterListNew.addFilter(colFilterNew);
            filterListNew.addFilter(filter);
            newScan.setFilter(filterListNew);
            ResultScanner scanner2 = fileTable.getScanner(newScan);

            Iterator<Result> res = scanner2.iterator();// 返回查询遍历器
            while (res.hasNext()) {
                Result result = res.next();
                FileInfoVO fileInfoVO = packageCells(result);
                fileInfoVOS.add(fileInfoVO);
            }

            tableModel.setData(200);
            tableModel.setData(fileInfoVOS);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileTable.close();
                userTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            } catch (Exception e) {
            }
        }
        return tableModel;
    }

    //更新用户存储文件总大小
    public static boolean insertOrUpdateUser(Table userTable, String size, String uid, String type) {
        try {
            //先获取当前用户存储的大小
            Get get = new Get(Bytes.toBytes(uid));
            get.addColumn(Bytes.toBytes(Static.USER_TABLE_CF), Bytes.toBytes(Static.USER_TABLE_SIZE));
            Result userRes = userTable.get(get);
            int KB = 1024;//定义GB的计算常量
            DecimalFormat df = new DecimalFormat("0.00");//格式化小数
            String resultSize = "";
            String newSize = null;
            resultSize = df.format(Integer.parseInt(size) / (float) KB);
            if (userRes.isEmpty()) {
                if (Double.parseDouble(resultSize) > 20 * 1024 * 1024) {
                    return false;
                }
                newSize = size;
            } else {
                String curSize = null;
                //先获得当前的存储大小
                for (Cell cell : userRes.rawCells()) {
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.USER_TABLE_SIZE)) {
                        curSize = Bytes.toString(CellUtil.cloneValue(cell));
                        String GSize = df.format(Double.parseDouble(curSize) / (float) KB);
                        //如果是up类型则判断是否超限
                        if (type.equals("upload")) {
                            if (Double.parseDouble(GSize) + Double.parseDouble(resultSize) > 20 * 1024 * 1024)
                                return false;
                            newSize = Double.parseDouble(size) + Double.parseDouble(curSize) + "";
                        } else {
                            newSize = Double.parseDouble(curSize) - Double.parseDouble(size) + "";
                        }
                    }
                }
            }
            Put put = new Put(Bytes.toBytes(uid));
            put.addColumn(Bytes.toBytes(Static.USER_TABLE_CF), Bytes.toBytes(Static.USER_TABLE_SIZE), Bytes.toBytes(newSize));
            userTable.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    //删除一个group最外层文件夹的所有文件
    public static boolean deleteFnHbase(String fileId, String gId, String uId) {
        Connection hbaseConnection = null;
        Table indexTable = null;
        Table groupTable = null;
        Table fileTable = null;
        try {
            hbaseConnection = HbaseConnectionPool.getHbaseConnection();
            indexTable = hbaseConnection.getTable(TableName.valueOf(Static.INDEX_TABLE));
            groupTable = hbaseConnection.getTable(TableName.valueOf(Static.GROUP_TABLE));
            fileTable = hbaseConnection.getTable(TableName.valueOf(Static.FILE_TABLE));

            //检查index表中所有的组中有没有该路径，如果有则删除该版本
            Get get = new Get(Bytes.toBytes(uId));
            get.setMaxVersions();
            if (gId != null) {
                get.addColumn(Bytes.toBytes(Static.INDEX_TABLE_CF), Bytes.toBytes(gId)); //如果删除的是共享组的某个文件
            } else {
                get.addFamily(Bytes.toBytes(Static.INDEX_TABLE_CF));
            }

            Result result = indexTable.get(get);
            List<Delete> deleteList = new ArrayList<>();
            List<String> authIdList = new ArrayList<>();
            List<Delete> indexDeList = new ArrayList<>();
            List<Delete> groupDeList = new ArrayList<>();
            for (Cell cell : result.rawCells()) {
                if (Bytes.toString(CellUtil.cloneValue(cell)).length() != 0) {
                    //如果fileId存在index表的该格子中，则删除该fileId的版本
                    if (Bytes.toString(CellUtil.cloneValue(cell)).equals(fileId)) {
                        Delete indexDelete = new Delete(Bytes.toBytes(uId));
                        indexDelete.addColumn(Bytes.toBytes(Static.INDEX_TABLE_CF), CellUtil.cloneQualifier(cell), cell.getTimestamp());
                        indexDeList.add(indexDelete);

                        //获取group表中该组的所以成员和文件id：（要删除组的节奏）遍历结果，如果是成员的列则封装成auth，必须删除每一个取得的auth
                        Get groupGet = new Get(CellUtil.cloneQualifier(cell));
                        groupGet.setMaxVersions();
                        groupGet.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_fileId));
                        groupGet.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_MEMBER));
                        Result groupRes = groupTable.get(groupGet);
                        //遍历该组的成员和fileid，获得所有的有要删除的文件权限的gid+uid
                        for (Cell rawCell : groupRes.rawCells()) {
                            if (Bytes.toString(CellUtil.cloneValue(rawCell)).length() != 0) {
                                //如果列名等于组成员并且列值不等于uid的话，说明不是此文件的拥有者，则gid+uid组合成权限值，准备删除
                                if (Bytes.toString(CellUtil.cloneQualifier(rawCell)).equals(Static.GROUP_TABLE_MEMBER) && !Bytes.toString(CellUtil.cloneValue(rawCell)).equals(uId)) {
                                    authIdList.add(gId + Bytes.toString(CellUtil.cloneValue(rawCell)));
                                }
                                //如果文件id等于当前要删除的fileId，则加入要删除的数组
                                if (Bytes.toString(CellUtil.cloneValue(rawCell)).equals(fileId)) {
                                    Delete groupDelete = new Delete(CellUtil.cloneRow(rawCell));
                                    groupDelete.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_fileId), rawCell.getTimestamp());
                                    groupDeList.add(groupDelete);
                                }
                            }
                        }
                    }
                }
            }

            //针对删除的文件夹获得其中的所有的fileId
            List<String> fielIdList=new ArrayList<>();
            fielIdList.add(fileId);
            //递归获取到该文件夹下所以要删除的文件
            deleteCallBack(fileTable,fielIdList,fileId,uId,gId);

            //查询file表获得对应的时间戳
            for (String fileid : fielIdList) {
                Get fileGet = new Get(Bytes.toBytes(fileid));
                fileGet.setMaxVersions();
                fileGet.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth));
                Result fileAuthRes = fileTable.get(fileGet);
                if (!fileAuthRes.isEmpty()) {
                    for (Cell cell : fileAuthRes.rawCells()) {
                        //如果包含了文件权限表的权限，则进行删除
                        if (authIdList.contains(Bytes.toString(CellUtil.cloneValue(cell)))) {
                            Delete delete = new Delete(Bytes.toBytes(fileid));
                            delete.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth), cell.getTimestamp());
                            deleteList.add(delete);
                        }
                    }
                }
            }
            indexTable.delete(indexDeList);
            groupTable.delete(groupDeList);
            fileTable.delete(deleteList);
            indexTable.close();
            fileTable.close();
            groupTable.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            if (hbaseConnection != null) {
                HbaseConnectionPool.releaseConnection(hbaseConnection);
            }
        }
        return true;
    }

    //递归获取要删除的文件夹下面所有的文件id
    private static void deleteCallBack(Table fileTable, List<String> fielIdList, String fileId, String uId, String gId) throws IOException {
        Scan scan = new Scan();
        FilterList filterList = new FilterList();
        Filter colFilter = new PrefixFilter(Bytes.toBytes(uId));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Static.FILE_TABLE_CF.getBytes(),
                Static.FILE_TABLE_BACK.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(fileId)));
        singleColumnValueFilter.setFilterIfMissing(true);
        filterList.addFilter(colFilter);
        filterList.addFilter(singleColumnValueFilter);

        scan.setFilter(filterList);
        ResultScanner scanner = fileTable.getScanner(scan);
        for (Result result : scanner) {
            String newFile = Bytes.toString(result.getRow());
             fielIdList.add(newFile);

            deleteCallBack(fileTable, fielIdList, newFile, uId, gId);
        }
    }




    //获取当前用户的共享组
    public static TableModel getAllFiles(String uid) throws Exception {
        Connection hBaseConn = null;
        try {
            if (StringUtils.isEmpty(uid)) {
                throw new Exception("参数有误");
            }
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //获得index表下面所有组的id
        Table indexTable = hBaseConn.getTable(TableName.valueOf(Static.INDEX_TABLE));
        Table groupTable = hBaseConn.getTable(TableName.valueOf(Static.GROUP_TABLE));
        Get indexGet = new Get(Bytes.toBytes(uid));
        indexGet.addFamily(Bytes.toBytes(Static.INDEX_TABLE_CF));
        Result result = indexTable.get(indexGet);
        List<GroupInfoVO> groupInfoVOList = new ArrayList<>();
        for (Cell cell : result.rawCells()) {
            GroupInfoVO groupInfoVO = new GroupInfoVO();
            Set<String> memberSet = new HashSet<>();
            Set<ShareFileVO> fileVOSet = new HashSet<>();
            //在group表中，根据组id，搜索出对应的name、path 和member
            Get groupGet = new Get(CellUtil.cloneQualifier(cell));
            groupInfoVO.setGId(Bytes.toString(CellUtil.cloneQualifier(cell)));
            groupGet.setMaxVersions();
            groupGet.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_NAME));
            groupGet.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_MEMBER));
            groupGet.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_fileId));
            Result groupRes = groupTable.get(groupGet);
            for (Cell rawCell : groupRes.rawCells()) {
                if (Bytes.toString(CellUtil.cloneQualifier(rawCell)).equals(Static.GROUP_TABLE_MEMBER)) //如果列名是member
                {
                    memberSet.add(Bytes.toString(CellUtil.cloneValue(rawCell)));
                } else if (Bytes.toString(CellUtil.cloneQualifier(rawCell)).equals(Static.GROUP_TABLE_NAME))  //如果列名name
                {
                    groupInfoVO.setName(Bytes.toString(CellUtil.cloneValue(rawCell)));
                } else {
                    String path = Bytes.toString(CellUtil.cloneValue(rawCell));
                    ShareFileVO shareFileVO = new ShareFileVO();
                    //截取最后一个/，之后的是文件名称
                    String fileName = path.substring(path.lastIndexOf("/") + 1);
                    shareFileVO.setName(fileName);
                    shareFileVO.setPath(path);
                    if (path.charAt(1) == '/' && path.charAt(10) == '/') {
                        shareFileVO.setSharer(path.substring(2, 10));
                    }
                    fileVOSet.add(shareFileVO);
                }
            }
            groupInfoVO.setMember(memberSet);
            groupInfoVO.setFile(fileVOSet);
            groupInfoVOList.add(groupInfoVO);
        }
        TableModel tableModel = new TableModel();
        tableModel.setData(groupInfoVOList);
        tableModel.setCode(200);
        indexTable.close();
        groupTable.close();
        HbaseConnectionPool.releaseConnection(hBaseConn);
        return tableModel;
    }


    //将当前用户共享的文件写入myshare表
    //更新相对于user的index表
    public static TableModel shareTo(ShareToFileVO shareToFileVO) {
        Connection hBaseConn = null;
        Table fileTable = null;
        Table groupTable = null;
        Table indexTable = null;
        //首先检查共享的文件是否已经存在myshare表中，如果存在则删除该版本数据数据，然后插入
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            groupTable = hBaseConn.getTable(TableName.valueOf(Static.GROUP_TABLE));
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            indexTable = hBaseConn.getTable(TableName.valueOf(Static.INDEX_TABLE));
            //检查index表和group表是否已经含有共享的路径
            List<Put> indexList = new ArrayList<>();
            List<Put> groupList = new ArrayList<>();
            Get indexGet = new Get(Bytes.toBytes(shareToFileVO.getUId()));
            indexGet.setMaxVersions();
            indexGet.addColumn(Bytes.toBytes(Static.INDEX_TABLE_CF), Bytes.toBytes(shareToFileVO.getGroupId()));
            Result indexRes = indexTable.get(indexGet);
            //标志位数组
            boolean[] rel = new boolean[shareToFileVO.getFileList().size()];
            for (Cell cell : indexRes.rawCells()) {
                for (int i = 0; i < shareToFileVO.getFileList().size(); i++) {
                    if (shareToFileVO.getFileList().get(i).equals(Bytes.toString(CellUtil.cloneValue(cell)))) {
                        rel[i] = true;
                    }
                }
            }
            //对没有被覆盖的进行插入
            long l = System.currentTimeMillis();
            for (int i = 0; i < rel.length; i++) {
                if (!rel[i]) {
                    Put indexPut = new Put(Bytes.toBytes(shareToFileVO.getUId()));
                    Put groupPut = new Put(Bytes.toBytes(shareToFileVO.getGroupId()));
                    indexPut.addColumn(Bytes.toBytes(Static.INDEX_TABLE_CF), Bytes.toBytes(shareToFileVO.getGroupId()), l + i, Bytes.toBytes(shareToFileVO.getFileList().get(i)));
                    indexList.add(indexPut);
                    groupPut.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_fileId), l + i, Bytes.toBytes(shareToFileVO.getFileList().get(i)));
                    groupList.add(groupPut);
                }
            }
            indexTable.put(indexList);
            groupTable.put(groupList);

            //对分享的文件进行授权：针对分享的每一个文件，先查询出共享组中的分组成员，然后把每一个id插入文件权限表中
            Get get = new Get(Bytes.toBytes(shareToFileVO.getGroupId()));
            get.setMaxVersions();
            get.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_MEMBER));
            Result result = groupTable.get(get);
            List<Cell> memberList = result.listCells();
            List<String> fileList = shareToFileVO.getFileList();
            List<Put> putList = new ArrayList<>();
            List<String> curAuthList = new ArrayList<>();
            for (int i = 0; i < fileList.size(); i++) {
                //先拿到该文件的所有的权限数组
                Get curAuthGet = new Get(Bytes.toBytes(fileList.get(i)));
                curAuthGet.setMaxVersions();
                curAuthGet.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth));
                Result authGet = fileTable.get(curAuthGet);
                for (Cell cell : authGet.rawCells()) {
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.FILE_TABLE_Auth)) {
                        curAuthList.add(Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }

                for (Cell menber : memberList) {
                    //先将当前文件放入list中
                    if (Bytes.toString(CellUtil.cloneValue(menber)).equals(shareToFileVO.getUId())) {
                        continue;
                    }
                    String memberId = Bytes.toString(CellUtil.cloneValue(menber));
                    if (curAuthList.contains(shareToFileVO.getGroupId() + memberId)) {
                        continue;
                    }
                    Put put = new Put(Bytes.toBytes(fileList.get(i)));
                    put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth),
                            System.currentTimeMillis(), Bytes.toBytes(shareToFileVO.getGroupId() + memberId));
                    putList.add(put);
                    shareCallBack(fileTable, putList, fileList.get(i), menber, shareToFileVO.getUId(), shareToFileVO.getGroupId());
                }
            }
            fileTable.put(putList);

        } catch (Exception e) {
            return TableModel.error("分享失败");
        } finally {
            try {
                indexTable.close();
                groupTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return TableModel.success("分享成功");
    }

    //递归对文件夹进行授权
    private static void shareCallBack(Table fileTable, List<Put> putList, String fileId, Cell menber, String uId, String gId) throws IOException {
        Scan scan = new Scan();
        FilterList filterList = new FilterList();
        Filter colFilter = new PrefixFilter(Bytes.toBytes(uId));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Static.FILE_TABLE_CF.getBytes(),
                Static.FILE_TABLE_BACK.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(fileId)));
        singleColumnValueFilter.setFilterIfMissing(true);
        filterList.addFilter(colFilter);
        filterList.addFilter(singleColumnValueFilter);

        scan.setFilter(filterList);
        ResultScanner scanner = fileTable.getScanner(scan);
        for (Result result : scanner) {
            String newFile = Bytes.toString(result.getRow());
            Put put = new Put(Bytes.toBytes(newFile));
            String memberId = Bytes.toString(CellUtil.cloneValue(menber));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth), System.currentTimeMillis(), Bytes.toBytes(gId + memberId));
            putList.add(put);
            shareCallBack(fileTable, putList, newFile, menber, uId, gId);
        }
    }


    //新建分组
    public static TableModel buildGroup(NewGroupInfoVO newGroupInfoVO) {
        Connection hBaseConn = null;
        Table groupTable = null;
        Table indexTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            groupTable = hBaseConn.getTable(TableName.valueOf(Static.GROUP_TABLE));
            indexTable = hBaseConn.getTable(TableName.valueOf(Static.INDEX_TABLE));

            //在group表中，将分组建立
            long l = System.currentTimeMillis();
            String groupId = newGroupInfoVO.getUId() + l;
            Put groupPut = new Put(Bytes.toBytes(groupId));
            groupPut.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_NAME), Bytes.toBytes(newGroupInfoVO.getGroupName()));

            for (int i = 0; i < newGroupInfoVO.getMember().size(); i++) {
                groupPut.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_MEMBER), l + i, Bytes.toBytes(newGroupInfoVO.getMember().get(i)));
            }
            groupTable.put(groupPut);

            //在index表中，针对每一个member，添加一个列
            List<Put> putList = new ArrayList<>();
            for (String member : newGroupInfoVO.getMember()) {
                Put indexPut = new Put(Bytes.toBytes(member));
                indexPut.addColumn(Bytes.toBytes(Static.INDEX_TABLE_CF), Bytes.toBytes(groupId), null);
                putList.add(indexPut);
            }
            indexTable.put(putList);
            return TableModel.success("创建成功");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                groupTable.close();
                indexTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //获得某一个分组的文件
    public static TableModel getGroupFile(String gId) {
        Connection hBaseConn = null;
        Table groupTable = null;
        Table fileTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            groupTable = hBaseConn.getTable(TableName.valueOf(Static.GROUP_TABLE));
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            //根据gid查询每个组的文件
            Get get = new Get(Bytes.toBytes(gId));
            get.setMaxVersions();
            get.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_fileId));
            Result result = groupTable.get(get);
            List<FileInfoVO> fileVOList = new ArrayList<>();
            for (Cell cell : result.rawCells()) {
                String fileId = Bytes.toString(CellUtil.cloneValue(cell));
                //根据fileId，查询出file表中对应的文件
                Get fileGet = new Get(Bytes.toBytes(fileId));
                fileGet.addFamily(Bytes.toBytes(Static.FILE_TABLE_CF));
                Result fileRes = fileTable.get(fileGet);
                if (!fileRes.isEmpty()) {
                    FileInfoVO fileInfoVO = packageCells(fileRes);
                    fileVOList.add(fileInfoVO);
                }
            }
            TableModel tableModel = new TableModel();
            tableModel.setData(fileVOList);
            tableModel.setCode(200);
            return tableModel;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                groupTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static TableModel getMyShare(String gId, String userId) {
        Connection hBaseConn = null;
        Table indexTable = null;
        Table fileTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            indexTable = hBaseConn.getTable(TableName.valueOf(Static.INDEX_TABLE));
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            //根据gid查询每个组的文件
            Get get = new Get(Bytes.toBytes(userId));
            get.setMaxVersions();
            get.addColumn(Bytes.toBytes(Static.INDEX_TABLE_CF), Bytes.toBytes(gId));
            Result result = indexTable.get(get);
            List<FileInfoVO> fileVOList = new ArrayList<>();
            for (Cell cell : result.rawCells()) {
                if (Bytes.toString(CellUtil.cloneValue(cell)).length() != 0) {
                    String fileId = Bytes.toString(CellUtil.cloneValue(cell));
                    //根据fileId从file表中查询到文件信息
                    Get fileGet = new Get(Bytes.toBytes(fileId));
                    fileGet.addFamily(Bytes.toBytes(Static.FILE_TABLE_CF));
                    Result fileRes = fileTable.get(fileGet);
                    if (!fileRes.isEmpty()) {
                        FileInfoVO fileInfoVO = packageCells(fileRes);
                        fileInfoVO.setMyShare(true);
                        fileVOList.add(fileInfoVO);
                    }
                }
            }
            TableModel tableModel = new TableModel();
            tableModel.setData(fileVOList);
            tableModel.setCode(200);
            return tableModel;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                indexTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //根据fileId在file表中查找文件信息
    public static TableModel selectFile(String backId, String type, String uId, String gId) {
        TableModel tableModel = new TableModel();
        Connection hBaseConn = null;
        Table fileTable = null;
        Table userTable = null;
        List<FileInfoVO> list = new ArrayList<>();
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            userTable = hBaseConn.getTable(TableName.valueOf(Static.USER_TABLE));
            //权限检测
            if (!verifite(fileTable, uId, backId, gId)) {
                return TableModel.error("您的访问权限不足");
            }
            Scan scan = new Scan();
            if (type.equals("0")) {
                //如果是返回上一级，则查询backid的backid，查询backid即可
                Get get = new Get(Bytes.toBytes(backId));
                get.setMaxVersions();
                get.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_BACK));
                Result result = fileTable.get(get);
                if (!result.isEmpty()) {
                    Cell cell = result.rawCells()[0];
                    String lastBack = Bytes.toString(CellUtil.cloneValue(cell));
                    HbaseConnectionPool.releaseConnection(hBaseConn);
                    return selectFile(lastBack, "1", uId, gId);
                }
            } else {
                SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                        Static.FILE_TABLE_CF.getBytes(),
                        Static.FILE_TABLE_BACK.getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(Bytes.toBytes(backId)));
                singleColumnValueFilter.setFilterIfMissing(true);
                scan.setFilter(singleColumnValueFilter);
                scan.setMaxVersions();
                ResultScanner results = fileTable.getScanner(scan);
                for (Result result : results) {
                    FileInfoVO fileInfoVO = packageCells(result);
                    list.add(fileInfoVO);
                }
            }
            //获取我的文件夹总大小
            Get get = new Get(Bytes.toBytes(uId));
            get.addColumn(Bytes.toBytes(Static.USER_TABLE_CF), Bytes.toBytes(Static.USER_TABLE_SIZE));
            Result result = userTable.get(get);
            if (!result.isEmpty()) {
                for (Cell cell : result.rawCells()) {
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.USER_TABLE_SIZE)) {
                        tableModel.setMsg(Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
            }
            tableModel.setData(list);
            tableModel.setCode(200);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            } catch (Exception e) {
            }
        }
        return tableModel;
    }

    //将查询出来的cell封装为返回list的方法
    public static FileInfoVO packageCells(Result result) {
        FileInfoVO fileInfoVO = new FileInfoVO();
        String fileId = null;
        for (Cell cell : result.rawCells()) {
            fileId = Bytes.toString(CellUtil.cloneRow(cell));
            if (fileId.length() == 0) {
                return null;
            }
            fileInfoVO.setFileId(fileId);

            fileInfoVO.setSharer(fileId.substring(0, 8));

            String re = Bytes.toString(CellUtil.cloneQualifier(cell));
            switch (re) {
                case "name":
                    fileInfoVO.setName(Bytes.toString(CellUtil.cloneValue(cell)));
                    break;
                case "path":
                    fileInfoVO.setPath(Bytes.toString(CellUtil.cloneValue(cell)));
                    break;
                case "size":
                    if (!Bytes.toString(CellUtil.cloneValue(cell)).equals("-")) {
                        fileInfoVO.setSize(Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell))));
                    } else {
                        fileInfoVO.setSize(null);
                    }
                    break;
                case "dir":
                    fileInfoVO.setDir(Bytes.toString(CellUtil.cloneValue(cell)).equals("true") ? true : false);
                    break;
                case "time":
                    fileInfoVO.setTime(Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell))));
                    break;
                case "back":
                    fileInfoVO.setBack(Bytes.toString(CellUtil.cloneValue(cell)));
                    break;
                case "auth":
                    break;
                default:
                    break;
            }
        }
        //设置类型
        if (!fileInfoVO.isDir()) {
            //截取最后的后缀名
            String name = fileInfoVO.getName();
            String substring = "";
            if (name.lastIndexOf(".") != -1) {
                substring = name.substring(name.lastIndexOf("."));
            }
            if (!substring.isEmpty()) {
                fileInfoVO.setType(substring);
            } else {
                fileInfoVO.setType("common");
            }
        } else {
            fileInfoVO.setType("dir");
        }
        fileInfoVO.setNew(false);
        return fileInfoVO;
    }


    //根据file表的文件id来查找，文件对应的物理路径
    public static String findUploadPath(String backId) throws IOException {
        Connection hBaseConn = null;
        Table fileTable = null;
        String path = null;
        if (backId.length() == 8) {
            path = Static.BASEURL + backId;
        } else {
            try {
                hBaseConn = HbaseConnectionPool.getHbaseConnection();
                fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
                //根据gid查询每个组的文件
                Get get = new Get(Bytes.toBytes(backId));
                get.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_PATH));
                Result result = fileTable.get(get);
                Cell cell = result.rawCells()[0];
                path = Bytes.toString(CellUtil.cloneValue(cell));
                fileTable.close();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            } finally {
                fileTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            }
        }
        return path;
    }

    //删除我的文件
    public static TableModel deleteFile(String fileId, String uId) throws IOException {
        if (!fileId.substring(0, 8).equals(uId)) {
            return TableModel.error("权限不足");
        }
        Connection hBaseConn = null;
        Table fileTable = null;
        Table userTable = null;
        List<Integer> sizeList = new ArrayList<>();
        String path = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            userTable = hBaseConn.getTable(TableName.valueOf(Static.USER_TABLE));
            //首先根据id查出文件物理路径，调用hdfs的方法删除文件
            Get get = new Get(Bytes.toBytes(fileId));
            get.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_PATH));
            get.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_SIZE));
            Result result = fileTable.get(get);
            if (!result.isEmpty()) {
                for (Cell cell : result.rawCells()) {
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.FILE_TABLE_PATH))
                        path = Bytes.toString(CellUtil.cloneValue(cell));
                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.FILE_TABLE_SIZE)) {
                        if (!Bytes.toString(CellUtil.cloneValue(cell)).equals("-")) {
                            sizeList.add(Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell))));
                        }
                    }
                }
                if (HBaseDao.delete(path, uId)) {
                    //当hdfs和共享组中删除完毕后，递归删除文件表中的该fileId下面的所有文件
                    List<Delete> deleteList = new ArrayList<>();
                    deleteFilesById(fileTable, deleteList, fileId, uId, sizeList);
                    deleteList.add(new Delete(Bytes.toBytes(fileId)));
                    if (!deleteList.isEmpty()) {
                        fileTable.delete(deleteList);
                    }
                    int sizeAll = 0;
                    for (Integer integer : sizeList) {
                        sizeAll += integer;
                    }
                    if (!insertOrUpdateUser(userTable, sizeAll + "", uId, "delete")) {
                        return TableModel.error("文件超出存储容量");
                    }
                }
            }
            return TableModel.success("删除成功");
        } catch (Exception e) {
            e.printStackTrace();
            return TableModel.error("删除失败");
        } finally {
            fileTable.close();
            HbaseConnectionPool.releaseConnection(hBaseConn);
        }
    }

    //递归删除某个fileId下面的全部文件的方法
    private static void deleteFilesById(Table fileTable, List<Delete> deleteList, String fileId, String uId, List sizeList) throws IOException {
        Scan scan = new Scan();
        //scan.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_SIZE));
        FilterList filterList = new FilterList();
        //Filter colFilter = new PrefixFilter(Bytes.toBytes(uId));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Static.FILE_TABLE_CF.getBytes(),
                Static.FILE_TABLE_BACK.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(fileId)));
        singleColumnValueFilter.setFilterIfMissing(true);
        //  filterList.addFilter(colFilter);
        filterList.addFilter(singleColumnValueFilter);

        scan.setFilter(filterList);
        ResultScanner scanner = fileTable.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.FILE_TABLE_SIZE)) {
                    if (!Bytes.toString(CellUtil.cloneValue(cell)).equals("-") && !Bytes.toString(CellUtil.cloneValue(cell)).isEmpty())
                        sizeList.add(Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            String newFile = Bytes.toString(result.getRow());
            deleteFnHbase(newFile, null, uId);
            Delete delete = new Delete(Bytes.toBytes(newFile));
            deleteList.add(delete);
            deleteFilesById(fileTable, deleteList, newFile, uId, sizeList);
        }
    }

    //下载方法
    public static void downLoad(String fileId, String gId, HttpServletResponse response, HttpServletRequest request, String uid) throws IOException {
        //从files表中查询出下载文件的物理地址，然后调用下载函数
        Connection hBaseConn = null;
        FileSystem fs = null;
        Table fileTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fs = HdfsConnectionPool.getHdfsConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            //权限验证
            if (!verifite(fileTable, uid, fileId, gId)) {
                return;
            }
            Get get = new Get(Bytes.toBytes(fileId));
            get.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_PATH));
            Result result = fileTable.get(get);
            if (!result.isEmpty()) {
                Cell cell = result.rawCells()[0];
                String path = Bytes.toString(CellUtil.cloneValue(cell));
                DownLoad.downloadFromHDFSinOffset(fs, response, path, request);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
                HdfsConnectionPool.releaseConnection(fs);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //获取文件类型
    public static TableModel getFilesByType(String type, String backId, String uId) {
        Connection hBaseConn = null;
        Table fileTable = null;
        try {
            if (!backId.substring(0, 8).equals(uId)) {
                return TableModel.error("权限不足");
            }
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            Scan scan = new Scan();
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            Filter colFilter = new PrefixFilter(Bytes.toBytes(uId));
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    Static.FILE_TABLE_CF.getBytes(),
                    Static.FILE_TABLE_TYPE.getBytes(),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(type)));
            singleColumnValueFilter.setFilterIfMissing(true);
            filterList.addFilter(colFilter);
            filterList.addFilter(singleColumnValueFilter);
            scan.setFilter(filterList);
            ResultScanner scanner = fileTable.getScanner(scan);
            List<FileInfoVO> list = new ArrayList<>();
            for (Result result : scanner) {
                FileInfoVO fileInfoVO = packageCells(result);
                list.add(fileInfoVO);
            }
            TableModel tableModel = new TableModel();
            tableModel.setData(list);
            tableModel.setCode(200);
            return tableModel;
        } catch (Exception e) {
            e.printStackTrace();
            return TableModel.error("参数有误");
        } finally {
            try {
                fileTable.close();
                HbaseConnectionPool.releaseConnection(hBaseConn);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    //查找框查找方法
    public static TableModel searchFile(String value, String uId) {
        Connection hBaseConn = null;
        Table fileTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            List<FileInfoVO> list = new ArrayList<>();
            Scan scan = new Scan();
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            Filter colFilter = new PrefixFilter(Bytes.toBytes(uId));

            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    Static.FILE_TABLE_CF.getBytes(),
                    Static.FILE_TABLE_NAME.getBytes(),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(value));
            singleColumnValueFilter.setFilterIfMissing(true);
            filterList.addFilter(colFilter);
            filterList.addFilter(singleColumnValueFilter);
            scan.setFilter(filterList);
            ResultScanner scanner = fileTable.getScanner(scan);
            for (Result result : scanner) {
                if (!result.isEmpty()) {
                    FileInfoVO fileInfoVO = packageCells(result);
                    list.add(fileInfoVO);
                }
            }

            TableModel tableModel = new TableModel();
            tableModel.setCode(200);
            tableModel.setData(list);
            return tableModel;
        } catch (Exception e) {
            e.printStackTrace();
            return TableModel.error("error");
        } finally {
            HbaseConnectionPool.releaseConnection(hBaseConn);
        }

    }

    //根据fileId和用户来校验权限
    public static boolean verifite(Table fileTable, String authId, String filedId, String gId) throws IOException {
        //通过fileTable查出该文件的权限信息
        //如果fileId为8位，则说明在查首页,只有本人能查到
        if ((filedId.length() == 8) || filedId.substring(0, 8).equals("00000000")) {
            return true;
        } else if (filedId.length() > 8) {

            Get get = new Get(Bytes.toBytes(filedId));
            get.setMaxVersions();
            get.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth));
            Result result = fileTable.get(get);
            List<Cell> cells = result.listCells();
            String newAuthId = null;
            if (!gId.isEmpty()) {
                newAuthId = gId + authId;
            }
            for (Cell cell : cells) {
                if (Bytes.toString(CellUtil.cloneValue(cell)).equals(newAuthId) || Bytes.toString(CellUtil.cloneValue(cell)).equals(authId) || Bytes.toString(CellUtil.cloneValue(cell)).equals("公开")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static TableModel reNameGroup(String gid, String reName, String uid) {
        Connection hBaseConn = null;
        Table groupTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            groupTable = hBaseConn.getTable(TableName.valueOf(Static.GROUP_TABLE));
            //检查用户
            if (!gid.substring(0, 8).equals(uid)) {
                return TableModel.error("权限不足");
            }
            //直接删除原名称，然后put新名称
            Delete delete = new Delete(Bytes.toBytes(gid));
            delete.addColumns(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_NAME));
            groupTable.delete(delete);

            //put新的值
            Put put = new Put(Bytes.toBytes(gid));
            put.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_NAME), Bytes.toBytes(reName));
            groupTable.put(put);

            TableModel tableModel = new TableModel();
            tableModel.setCode(200);
            return tableModel;
        } catch (Exception e) {
            e.printStackTrace();
            return TableModel.error("error");
        } finally {
            HbaseConnectionPool.releaseConnection(hBaseConn);
        }
    }

    public static TableModel deleteGroup(String gid, String uid) {
        Connection hBaseConn = null;
        Table groupTable = null;
        Table indexTable = null;
        Table fileTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            groupTable = hBaseConn.getTable(TableName.valueOf(Static.GROUP_TABLE));
            indexTable = hBaseConn.getTable(TableName.valueOf(Static.INDEX_TABLE));
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            //检查用户
            if (!gid.substring(0, 8).equals(uid)) {
                return TableModel.error("权限不足");
            }
            //先获取所有的组成员，和组分享文件
            List<String> memberList = new ArrayList<>();
            List<String> fileList = new ArrayList<>();

            Get memberGet = new Get(Bytes.toBytes(gid));
            memberGet.setMaxVersions();
            memberGet.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_MEMBER));
            memberGet.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_fileId));
            Result result = groupTable.get(memberGet);
            for (Cell cell : result.rawCells()) {
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.GROUP_TABLE_MEMBER)) {
                    memberList.add(Bytes.toString(CellUtil.cloneValue(cell)));
                } else if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.GROUP_TABLE_fileId)) {
                    fileList.add(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            //每个member在index表中删除列的所有版本
            List<Delete> indexDeleteList = new ArrayList<>();
            if (memberList.size() != 0) {
                for (String memberId : memberList) {
                    Delete delete = new Delete(Bytes.toBytes(memberId));
                    delete.addColumns(Bytes.toBytes(Static.INDEX_TABLE_CF), Bytes.toBytes(gid));
                    indexDeleteList.add(delete);
                }
            }
            indexTable.delete(indexDeleteList);

            //遍历所有的组的分享文件，删去所有的该组开头的权限名
            List<Delete> authDeleteList = new ArrayList<>();
            if (fileList.size() != 0) {
                for (String fileId : fileList) {
                    //查询file中该文件的权限的时间戳
                    Get authGet = new Get(Bytes.toBytes(fileId));
                    authGet.setMaxVersions();
                    authGet.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth));
                    Result fileRes = fileTable.get(authGet);
                    for (Cell cell : fileRes.rawCells()) {
                        //针对每一个gid开头权限的时间戳生成delete
                        if (Bytes.toString(CellUtil.cloneValue(cell)).contains(gid) && !Bytes.toString(CellUtil.cloneValue(cell)).equals(uid)) {
                            Delete authDelete = new Delete(Bytes.toBytes(fileId));
                            authDelete.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth), cell.getTimestamp());
                            authDeleteList.add(authDelete);
                        }
                    }
                    fileTable.delete(authDeleteList);
                }
            }

            //在group组中删去该组
            Delete groupDelete = new Delete(Bytes.toBytes(gid));
            groupTable.delete(groupDelete);

            TableModel tableModel = new TableModel();
            tableModel.setCode(200);
            indexTable.close();
            fileTable.close();
            groupTable.close();
            return tableModel;
        } catch (Exception e) {
            e.printStackTrace();
            return TableModel.error("error");
        } finally {
            HbaseConnectionPool.releaseConnection(hBaseConn);
        }
    }

    public static TableModel getMembersBygid(String gid, String uid) {
        Connection hBaseConn = null;
        Table groupTable = null;
        try {
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            groupTable = hBaseConn.getTable(TableName.valueOf(Static.GROUP_TABLE));

            List<MembersInfo> members = new ArrayList<>();
            Get get = new Get(Bytes.toBytes(gid));
            get.setMaxVersions();
            get.addColumn(Bytes.toBytes(Static.GROUP_TABLE_CF), Bytes.toBytes(Static.GROUP_TABLE_MEMBER));
            Result result = groupTable.get(get);
            for (Cell cell : result.rawCells()) {
                if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(Static.GROUP_TABLE_MEMBER)) {
                    MembersInfo membersInfo = new MembersInfo();
                    membersInfo.setUid(Bytes.toString(CellUtil.cloneValue(cell)));
                    members.add(membersInfo);
                }
            }
            TableModel tableModel = new TableModel();
            tableModel.setData(members);
            tableModel.setCode(200);
            return tableModel;
        } catch (Exception e) {
            e.printStackTrace();
            return TableModel.error("error");
        } finally {
            HbaseConnectionPool.releaseConnection(hBaseConn);
        }
    }


    //目录的删除
    public static boolean delete(String detSrc, String uId) {
        FileSystem fs = null;
        //true意思是递归删除，如果不为空也删除
        try {
            fs = HdfsConnectionPool.getHdfsConnection();
            fs.delete(new Path(detSrc), true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                HdfsConnectionPool.releaseConnection(fs);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
    }



    //目录的创建
    public static TableModel mkdir(String backId, String dirName, String userId) throws IOException {
        if (backId.length()>8)
        {
            if (!backId.substring(0, 8).equals(userId)) {
                return TableModel.error("权限不足");
            }
        }
        String path = HBaseDao.findUploadPath(backId);
        if (!path.isEmpty()) {
            path = path + "/" + dirName;
        }
        FileSystem fs = null;
        try {
            fs = HdfsConnectionPool.getHdfsConnection();
            fs.mkdirs(new Path(path));
            HBaseDao.insertToFiles(null, "dir", path, backId, userId, userId + "_" + System.currentTimeMillis());
            HdfsConnectionPool.releaseConnection(fs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return TableModel.success("创建成功");
    }


}

