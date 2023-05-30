import axios from "axios";
import { BASEURL } from "../baseURL";

export const GET_DATA = 'GET_DATA';

export const getResponse = (data) => {
  return {
    type: "GET_DATA",
    data: data.results,
    paginations: data.pagination,

  }
}


export const getData = (perpage, page) => dispatch => {
  return axios.get(`${BASEURL}/?per-page=${perpage}&page=${page}`)
    .then(function (response) {
      dispatch(getResponse(response.data))
      
    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
}

export const DeleteData = (perpage, page) => dispatch => {
  return axios.get(`${BASEURL}/?per-page=${perpage}&page=${page}&delete=1`)
    .then(function (response) {
      dispatch(getResponse(response.data))

    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
}