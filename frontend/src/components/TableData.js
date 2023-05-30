import React, { useEffect } from 'react';
import data from './data.js'
import axios from 'axios';
import "./style.css";
import { BASEURL } from '../baseURL.js';
import { DeleteData, getData } from '../Action/index'
import { useDispatch, useSelector } from "react-redux";


const TableData = () => {
    const dispatch = useDispatch();
    const Data = useSelector((state) => state.ApiReducer.data)
    const getpagination = useSelector((state) => state.ApiReducer.paginations)
    const [pagenumbers, setpagenumbers] = React.useState([1, 2, 3, 4, 5])
    const [tabledata, setTabledata] = React.useState([])
    const [pagination, setPagination] = React.useState([])
    const [perpage, setPerpage] = React.useState(10)
    const [page, setPage] = React.useState(1)

    React.useEffect(() => {
        dispatch(getData(perpage, page))
    }, [page, perpage])

    useEffect(() => {
        if (Data !== null && Data!== undefined) {
            setTabledata(Data)
            setPagination(getpagination)
        }
    }, [Data])

    const Newpagenumbers = (currentpage) => {
        if (pagenumbers.indexOf(currentpage) == 4 || pagenumbers.indexOf(currentpage) == 0) {
            let newpagenumbers = [currentpage - 2, currentpage - 1, currentpage, currentpage + 1, currentpage + 2]
            setpagenumbers(newpagenumbers)
        }
    }

    const jumpPage = (e) => {
        let p = parseInt(e.target.innerHTML)
        setPage(p)
        if (p != pagination.pages && p != 1) {
            Newpagenumbers(p)
        }
    }

    const nextJump = (e) => {
        let p = page + 1
        if (page != pagination.pages) {
            setPage(p)
            Newpagenumbers(p)
        }
    }

    const previousJump = (e) => {
        let p = page + 1
        if (page != 0) {
            setPage(page - 1)
            Newpagenumbers(p)
        }
    }

    const PerPageHandler = (e) => {
        setTabledata([])
        setPerpage(parseInt(e.target.value))
    }

    const deletedata = () => {
        setTabledata([])
        dispatch(DeleteData(perpage, page))
    }

    return (
        <div className='container mt-5'>

            <div id="wrap">
                <div class="container">
                    <h3>Data table</h3>
                    <div className='deleteBtnStyle'>
                        <button onClick={deletedata} className='dltBtn'>Refresh</button>
                    </div>
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>IMDB Id</th>
                                <th>Movie</th>
                                <th>Movie publish date(s)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {
                                (tabledata.length != 0) ? tabledata.map((el, index) => {
                                    return (
                                        <tr key={index}>
                                            <td>{el.id}</td>
                                            <td>{el.imdb_id}</td>
                                            <td>{el.movie}</td>
                                            <td>{el.movie_pubdate.slice(0,10)}</td>
                                        </tr>
                                    )
                                })
                                    : null
                            }
                        </tbody>
                    </table>

                    {
                        (tabledata.length == 0) ?
                            <h5 className='mt-5' style={{ textAlign: "center" }}>Loading...</h5>
                            : null
                    }

                    {/* Pagination */}
                    {
                        (tabledata.length != 0) ?
                            <div >
                                <div style={{ display: "flex", justifyContent: "end" }}>
                                    <label for="itemsPerPage">Items per Page:</label>
                                    <select class="form-select mb-2"
                                        aria-label="Items per Page"
                                        style={{ width: "100px", height: "38px", margin: "0 10px" }}
                                        onChange={PerPageHandler}
                                        value={perpage}
                                    >
                                        <option value="10">10</option>
                                        <option value="20">20</option>
                                        <option value="25">25</option>
                                        <option value="50">50</option>
                                    </select>
                                    <ul class="pagination justify-content-end">
                                        <li class="page-item page-link"
                                            onClick={previousJump}
                                            style={{ cursor: "pointer" }}
                                        >
                                            Previous
                                        </li>

                                        {
                                            pagenumbers.map((el) => {
                                                return (
                                                    <li className={el == page ? " page-link active" : "page-item page-link"}
                                                        style={{ cursor: "pointer" }}
                                                        onClick={jumpPage}
                                                    >
                                                        {el}
                                                    </li>
                                                )
                                            })
                                        }
                                        <li class="page-item page-link"
                                            onClick={nextJump}
                                            style={{ cursor: "pointer" }}
                                        >
                                            Next
                                        </li>
                                    </ul>
                                </div>

                            </div>
                            : null
                    }

                </div>
            </div>
        </div>
    )
}

export default TableData;